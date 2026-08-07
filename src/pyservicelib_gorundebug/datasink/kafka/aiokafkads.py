#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Optional, Protocol, Callable, Any, cast

from aiokafka import AIOKafkaProducer  # type: ignore[import-not-found,import-untyped]

from ...runtime.common import (
    Consumer, TypedSinkStream, ServiceExecutionEnvironment, Stream,
    SinkEndpoint, OutputEndpointConsumer,
)
from ...runtime.context import Context
from ...runtime.context.request import with_stream_id
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint
from ...runtime.environment.tracing import (
    Tracer, start_endpoint_span, span_event, span_error, string_attr,
)


class Partitioner[T](Protocol):
    """Controls which Kafka partition a message lands on. Equivalent to Go's Partitioner."""
    def partition(self, value: T, num_partitions: int) -> int: ...


class SinkMessage[R]:
    """
    Passed to EndpointHandler.consume_message.
    Set key and value, then call send() or skip().
    Equivalent to Go's datasink/kafka SinkMessage.
    """

    key: Optional[bytes]
    value: Optional[bytes]
    _topic: str
    _send_fn: Callable
    _result_collect: Callable[[R], Any]

    def __init__(
        self,
        topic: str,
        send_fn: Callable,
        result_collect: Callable[[R], Any],
    ):
        self.key = None
        self.value = None
        self._topic = topic
        self._send_fn = send_fn
        self._result_collect = result_collect

    def topic(self) -> str:
        return self._topic

    def send(
        self,
        on_delivery: Callable[[int, int, Optional[Exception]], R],
    ) -> None:
        """Publish key/value asynchronously. on_delivery converts delivery result to R."""

        result_collect = self._result_collect

        def _delivery(partition: int, offset: int, err: Optional[Exception]) -> None:
            result = on_delivery(partition, offset, err)
            # Schedule delivery of the result into the pipeline
            coro = result_collect(result)
            if asyncio.iscoroutine(coro):
                asyncio.ensure_future(coro)

        self._send_fn(self.key, self.value, _delivery)

    async def send_sync(self) -> "tuple[int, int, Optional[Exception]]":
        """Publish and block until delivery confirmed. Returns (partition, offset, error)."""
        done: asyncio.Future[tuple[int, int, Optional[Exception]]] = (
            asyncio.get_event_loop().create_future()
        )

        def _delivery(partition: int, offset: int, err: Optional[Exception]) -> None:
            if not done.done():
                done.set_result((partition, offset, err))

        self._send_fn(self.key, self.value, _delivery)
        return await done

    async def out(self, result: R) -> None:
        """Push result directly into the pipeline result stream."""
        coro = self._result_collect(result)
        if asyncio.iscoroutine(coro):
            await coro

    async def skip(self, result: R) -> None:
        """Push result without sending to Kafka."""
        await self.out(result)


class EndpointHandler[HandlerState, T, R](Protocol):
    """
    User-supplied handler for Kafka sink messages.

    Lifecycle:
        get_stream_id → begin_request → consume_message → end_request

    Equivalent to Go's datasink/kafka EndpointHandler.
    """

    def get_stream_id(self, value: T) -> str: ...

    def begin_request(
        self,
        stream: Stream,
    ) -> HandlerState: ...

    async def consume_message(
        self,
        stream: Stream,
        handler_state: HandlerState,
        value: T,
        msg: SinkMessage[R],
    ) -> None: ...

    async def end_request(
        self,
        stream: Stream,
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


class _AIOKafkaSinkDataSink(OutputDataSink):
    _producer: Optional[AIOKafkaProducer]
    _stopped: bool

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)
        self._producer = None
        self._stopped = False

    async def start(self, ctx: Context) -> None:
        cfg_ds = self.data_connector
        brokers = getattr(cfg_ds, 'brokers', None) or 'localhost:9092'

        for ep in self.endpoints:
            await cast("_AIOKafkaSinkEndpoint", ep).start(ctx)

        producer = AIOKafkaProducer(bootstrap_servers=brokers)
        await producer.start()
        self._producer = producer

    async def stop(self, ctx: Context) -> None:
        self._stopped = True
        if self._producer is not None:
            await self._producer.stop()
        for ep in self.endpoints:
            await cast("_AIOKafkaSinkEndpoint", ep).stop(ctx)

    async def send_message(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        on_delivery: Callable[[int, int, Optional[Exception]], None],
    ) -> None:
        if self._producer is None or self._stopped:
            on_delivery(0, 0, RuntimeError("Kafka producer is stopped or not started"))
            return
        try:
            fut = await self._producer.send(topic, value=value, key=key)
            record_metadata = await fut
            on_delivery(record_metadata.partition, record_metadata.offset, None)
        except Exception as e:
            on_delivery(0, 0, e)


class _AIOKafkaSinkEndpoint(DataSinkEndpoint):
    _consumer_obj: Optional["_AIOKafkaEndpointConsumer"]
    topic: str

    def __init__(self, data_sink: _AIOKafkaSinkDataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)
        self._consumer_obj = None
        cfg = data_sink.environment.config.get_endpoint_config_by_id(id_endpoint)
        self.topic = getattr(cfg, 'topic', '') or ''

    async def start(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.stop(ctx)


class _AIOKafkaEndpointConsumer[HandlerState, T, R](Consumer[T], OutputEndpointConsumer):
    _endpoint: _AIOKafkaSinkEndpoint
    _stream: TypedSinkStream[T, R]
    _handler: EndpointHandler[HandlerState, T, R]
    _partitioner: Optional[Partitioner[T]]
    _tracer: Optional[Tracer]

    def __init__(
        self,
        endpoint: _AIOKafkaSinkEndpoint,
        stream: TypedSinkStream[T, R],
        handler: EndpointHandler[HandlerState, T, R],
        partitioner: Optional[Partitioner[T]] = None,
        tracer: Optional[Tracer] = None,
    ):
        self._endpoint = endpoint
        self._stream = stream
        self._handler = handler
        self._partitioner = partitioner
        self._tracer = tracer

        stream.set_sink_consumer(self)
        endpoint._consumer_obj = self
        endpoint.add_endpoint_consumer(self)

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

    async def start(self, ctx: Context) -> None:
        pass

    async def stop(self, ctx: Context) -> None:
        pass

    async def consume(self, value: T) -> None:
        stream = self._stream
        sid = self._handler.get_stream_id(value)
        with_stream_id(sid)

        ep = self._endpoint
        _, span = start_endpoint_span(
            self._tracer,
            "kafka.output",
            stream.name,
            ep.name,
            "stream_id",
            sid,
        )
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                handler_state = self._handler.begin_request(stream)
                span_event(span, "begin_request")

                ds = cast(_AIOKafkaSinkDataSink, ep.datasink)

                async def _result_collect(r: R) -> None:
                    if hasattr(stream, 'error_stream'):
                        await stream.error_stream.consume(r)  # type: ignore[arg-type]

                def _send_fn(
                    key: Optional[bytes],
                    val: Optional[bytes],
                    on_delivery: Callable,
                ) -> None:
                    asyncio.create_task(ds.send_message(ep.topic, key, val, on_delivery))

                msg: SinkMessage[R] = SinkMessage[R](
                    topic=ep.topic,
                    send_fn=_send_fn,
                    result_collect=_result_collect,
                )

                try:
                    await self._handler.consume_message(stream, handler_state, value, msg)
                    span_event(span, "consume_message")
                    await self._handler.end_request(stream, None, handler_state)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    await self._handler.end_request(stream, err, handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()


def _make_tracer(stream: TypedSinkStream, env: ServiceExecutionEnvironment) -> Optional[Tracer]:
    tracing = env.tracing
    if tracing is None:
        return None
    return tracing.tracer(env.service_config.name)


def make_aiokafka_endpoint_consumer[HandlerState, T, R](
    stream: TypedSinkStream[T, R],
    handler: "EndpointHandler[HandlerState, T, R]",
    partitioner: Optional["Partitioner[T]"] = None,
) -> Consumer[T]:
    """
    Creates an aiokafka sink endpoint consumer.
    Equivalent to Go's MakeSaramaKafkaEndpointConsumer (datasink/kafka).
    """
    env = stream.environment
    cfg_ep = env.config.get_endpoint_config_by_id(stream.endpoint_id)

    datasink = env.get_datasink(cfg_ep.id_data_connector)
    if datasink is None:
        cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
        datasink = _AIOKafkaSinkDataSink(connector_id=cfg_ds.id, env=env)
        env.add_datasink(datasink)
    ds = cast(_AIOKafkaSinkDataSink, datasink)

    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is None:
        endpoint = _AIOKafkaSinkEndpoint(data_sink=ds, id_endpoint=stream.endpoint_id)
        ds.add_endpoint(endpoint)

    return _AIOKafkaEndpointConsumer[HandlerState, T, R](
        endpoint=cast(_AIOKafkaSinkEndpoint, endpoint),
        stream=stream,
        handler=handler,
        partitioner=partitioner,
        tracer=_make_tracer(stream, env),
    )

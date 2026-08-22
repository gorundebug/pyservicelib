#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import random
from typing import Optional, Protocol, Callable, Any, cast

from aiokafka import AIOKafkaProducer  # type: ignore[import-not-found,import-untyped]
from aiokafka.admin import (  # type: ignore[import-not-found,import-untyped]
    AIOKafkaAdminClient,
    NewTopic,
)
from aiokafka.errors import (  # type: ignore[import-not-found,import-untyped]
    TopicAlreadyExistsError,
    for_code,
)

from ...runtime.common import (
    Consumer, TypedSinkStream, ServiceExecutionEnvironment, Stream,
    SinkEndpoint, OutputEndpointConsumer,
)
from ...runtime.context import Context
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
    _create_task: Callable[..., None]

    def __init__(
        self,
        topic: str,
        send_fn: Callable,
        result_collect: Callable[[R], Any],
        create_task: Callable[..., None],
    ):
        self.key = None
        self.value = None
        self._topic = topic
        self._send_fn = send_fn
        self._result_collect = result_collect
        self._create_task = create_task

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
            self._create_task(result_collect, result)

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
        for ep in self.endpoints:
            await cast("_AIOKafkaSinkEndpoint", ep).start(ctx)
        enabled = [
            cast("_AIOKafkaSinkEndpoint", ep)
            for ep in self.endpoints
            if cast("_AIOKafkaSinkEndpoint", ep).enabled
        ]
        if not enabled:
            return

        cfg_ds = self.data_connector
        brokers = getattr(cfg_ds, 'brokers', None) or ''
        if not brokers:
            raise ValueError(f"No brokers configured for Kafka data sink '{self.name}'")

        await _create_topics(brokers, [ep.config for ep in enabled], cfg_ds)

        producer = AIOKafkaProducer(
            bootstrap_servers=brokers, **_client_options(cfg_ds)
        )
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
        partition: Optional[int],
        on_delivery: Callable[[int, int, Optional[Exception]], None],
    ) -> None:
        if self._producer is None or self._stopped:
            on_delivery(0, 0, RuntimeError("Kafka producer is stopped or not started"))
            return
        try:
            fut = await self._producer.send(
                topic, value=value, key=key, partition=partition
            )
            record_metadata = await fut
            on_delivery(record_metadata.partition, record_metadata.offset, None)
        except Exception as e:
            on_delivery(0, 0, e)

    async def partitions_for(self, topic: str) -> int:
        if self._producer is None or self._stopped:
            raise RuntimeError("Kafka producer is stopped or not started")
        partitions = await self._producer.partitions_for(topic)
        if not partitions:
            raise RuntimeError(f"Kafka topic {topic!r} has no partitions")
        return len(partitions)


class _AIOKafkaSinkEndpoint(DataSinkEndpoint):
    _consumer_obj: Optional["_AIOKafkaEndpointConsumer"]
    topic: str
    enabled: bool

    def __init__(self, data_sink: _AIOKafkaSinkDataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)
        self._consumer_obj = None
        self.enabled = False
        cfg = data_sink.environment.config.get_endpoint_config_by_id(id_endpoint)
        self.topic = getattr(cfg, 'topic', '') or ''

    async def start(self, ctx: Context) -> None:
        cfg = self.environment.config.get_endpoint_config_by_id(self.id)
        self.enabled = bool(getattr(cfg, 'enabled', False))
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
        self._enabled = False

        stream.set_sink_consumer(self)
        endpoint._consumer_obj = self
        endpoint.add_endpoint_consumer(self)

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

    async def start(self, ctx: Context) -> None:
        self._enabled = self._endpoint.enabled

    async def stop(self, ctx: Context) -> None:
        self._enabled = False

    async def consume(self, value: T) -> None:
        if not getattr(self, "_enabled", True):
            return
        stream = self._stream
        sid = self._handler.get_stream_id(value)

        ep = self._endpoint
        _, span = start_endpoint_span(
            self._tracer,
            "kafka.output",
            stream.name,
            ep.name,
            "stream_id",
            sid,
        )
        start_time: Optional[float] = None
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                try:
                    handler_state = self._handler.begin_request(stream)
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(
                        span,
                        "begin_request.error",
                        string_attr("error", str(err)),
                    )
                    return
                span_event(span, "begin_request")
                start_time = ep.on_request_start()

                ds = cast(_AIOKafkaSinkDataSink, ep.datasink)

                async def _result_collect(r: R) -> None:
                    if hasattr(stream, 'error_stream'):
                        await stream.error_stream.consume(r)  # type: ignore[arg-type]

                def _send_fn(
                    key: Optional[bytes],
                    val: Optional[bytes],
                    on_delivery: Callable,
                ) -> None:
                    async def _send_message() -> None:
                        try:
                            partitions = await ds.partitions_for(ep.topic)
                            partition = (
                                self._partitioner.partition(value, partitions)
                                if self._partitioner is not None
                                else random.randrange(partitions)
                            )
                        except Exception as err:
                            on_delivery(0, 0, err)
                            return
                        await ds.send_message(
                            ep.topic, key, val, partition, on_delivery
                        )

                    ep.environment.runtime.create_task(
                        _send_message,
                    )

                msg: SinkMessage[R] = SinkMessage[R](
                    topic=ep.topic,
                    send_fn=_send_fn,
                    result_collect=_result_collect,
                    create_task=ep.environment.runtime.create_task,
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
            if start_time is not None:
                ep.on_request_end(start_time, end_err)
            span.end()


def _make_tracer(stream: TypedSinkStream, env: ServiceExecutionEnvironment) -> Optional[Tracer]:
    tracing = env.tracing
    if tracing is None:
        return None
    return tracing.tracer(env.service_config.name)


def _client_options(config: Any) -> dict[str, Any]:
    # Current aiokafka always negotiates the broker protocol; api_version is a
    # deprecated no-op. request_timeout_ms is its public connection/request
    # timeout control and uses the same millisecond unit as Go's dialTimeout.
    dial_timeout = float(getattr(config, "dial_timeout", 0.0) or 0.0)
    result: dict[str, Any] = ({"request_timeout_ms": max(1, int(dial_timeout))}
                              if dial_timeout > 0 else {})
    protocol = getattr(config, "security_protocol", None)
    protocol = getattr(protocol, "value", protocol) or "PLAINTEXT"
    result["security_protocol"] = protocol
    if protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        username = getattr(config, "username", None)
        password = getattr(config, "password", None)
        if not username or not password:
            raise ValueError("Kafka SASL username and password must both be configured")
        mechanism = getattr(config, "sasl_mechanism", None)
        result["sasl_mechanism"] = getattr(mechanism, "value", mechanism) or "PLAIN"
        result["sasl_plain_username"] = username
        result["sasl_plain_password"] = password
    return result


async def _create_topics(
    brokers: str, endpoint_configs: list[Any], connector_config: Any = None
) -> None:
    topics: list[NewTopic] = []
    names: set[str] = set()
    for cfg in endpoint_configs:
        topic = getattr(cfg, "topic", "") or ""
        if not getattr(cfg, "create_topic", False) or topic in names:
            continue
        if not topic:
            raise ValueError("Kafka endpoint configured to create an empty topic")
        names.add(topic)
        topics.append(
            NewTopic(
                name=topic,
                num_partitions=max(1, int(getattr(cfg, "partitions", 0) or 0)),
                replication_factor=max(
                    1, int(getattr(cfg, "replication_factor", 0) or 0)
                ),
            )
        )
    if not topics:
        return

    admin = AIOKafkaAdminClient(
        bootstrap_servers=brokers, **_client_options(connector_config)
    )
    await admin.start()
    try:
        response = await admin.create_topics(topics)
        for topic_error in response.topic_errors:
            topic, error_code, *messages = topic_error
            if error_code in (0, TopicAlreadyExistsError.errno):
                continue
            message = messages[0] if messages else ""
            error_type = for_code(error_code)
            raise error_type(f"Could not create Kafka topic {topic!r}: {message}")
    finally:
        await admin.close()


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

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Optional, Protocol, Any, cast

from aiokafka import AIOKafkaConsumer  # type: ignore[import-not-found]
from aiokafka.structs import ConsumerRecord  # type: ignore[import-not-found]

from ...runtime.common import (
    TypedInputStream, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.environment.tracing import (
    Tracer, Span, start_span, span_event, span_error, string_attr, sampling_enabled,
)

_PENDING_ROTATION_INTERVAL = 30.0  # seconds


class ConsumerMessage:
    """
    Wraps an aiokafka ConsumerRecord with Commit/MarkMessage helpers.
    Equivalent to Go's datasource/kafka ConsumerMessage.
    """

    key: Optional[bytes]
    value: Optional[bytes]
    topic: str
    partition: int
    offset: int

    _record: ConsumerRecord
    _consumer: AIOKafkaConsumer

    def __init__(self, record: ConsumerRecord, consumer: AIOKafkaConsumer):
        self._record = record
        self._consumer = consumer
        self.key = record.key
        self.value = record.value
        self.topic = record.topic
        self.partition = record.partition
        self.offset = record.offset

    async def commit(self) -> None:
        """Manually commit this message's offset."""
        from aiokafka import TopicPartition  # type: ignore[import-untyped]
        tp = TopicPartition(self.topic, self.partition)
        await self._consumer.commit({tp: self.offset + 1})


class ResultCallback[HandlerState, T, R, E](Protocol):
    """Return True to deregister; False to keep active. Equivalent to Go's kafka ResultCallback."""
    def __call__(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
    ) -> bool: ...


class ResultContext[HandlerState, T, R, E]:
    """Holds result callbacks for a single Kafka message. Equivalent to Go's kafka ResultContext."""

    _handler_state: HandlerState
    _done: asyncio.Event
    _callbacks: dict[str, Any]
    _cb_lock: asyncio.Lock
    _span: Optional[Span]
    _once: bool

    def __init__(self, handler_state: HandlerState):
        self._handler_state = handler_state
        self._done = asyncio.Event()
        self._callbacks = {}
        self._cb_lock = asyncio.Lock()
        self._span = None
        self._once = False

    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, R, E]",
    ) -> None:
        self._callbacks[message_id] = cb

    def done(self) -> None:
        if not self._once:
            self._once = True
            span_event(self._span, "done_called")
        self._done.set()


class EndpointHandler[HandlerState, T, R, E](Protocol):
    """
    User-supplied handler for Kafka source messages.

    Lifecycle with result stream:
        begin_request → consume_message → [await done] → end_request

    Lifecycle without result stream:
        begin_request → consume_message → end_request

    concurrency() controls maximum parallel message processing (0 = unlimited).
    Equivalent to Go's datasource/kafka EndpointHandler.
    """

    def concurrency(self, sc: StreamContext[T, R, E]) -> int: ...

    async def begin_request(
        self,
        sc: StreamContext[T, R, E],
    ) -> HandlerState: ...

    async def consume_message(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        msg: ConsumerMessage,
        result_ctx: "ResultContext[HandlerState, T, R, E]",
    ) -> None: ...

    def get_message_id(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
    ) -> str: ...

    async def end_request(
        self,
        sc: StreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


class _AIOKafkaDataSource(InputDataSource):

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast("_AIOKafkaEndpoint", ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_coros = [
            cast("_AIOKafkaEndpoint", ep).stop(ctx)
            for ep in self.endpoints
        ]
        if stop_coros:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_coros, return_exceptions=True),
                    timeout=ctx.time_left,
                )
            except (asyncio.TimeoutError, TypeError):
                self.environment.log.warn(
                    f"AIOKafka data source '{self.name}' stopped by timeout."
                )


class _AIOKafkaEndpoint(DataSourceEndpoint):
    _consumer_obj: Optional["_AIOKafkaTypedEndpointConsumer"]

    def __init__(self, datasource: _AIOKafkaDataSource, id_endpoint: int):
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        self._consumer_obj = None

    async def start(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.stop(ctx)


class _ResultConsumerProxy[R](Consumer[R]):
    def __init__(self, consumer: "_AIOKafkaTypedEndpointConsumer") -> None:  # type: ignore[type-arg]
        self._consumer = consumer

    async def consume(self, value: R) -> None:
        await self._consumer._consume_result(value)  # type: ignore[arg-type]


class _AIOKafkaTypedEndpointConsumer[HandlerState, T, R, E](DataSourceEndpointConsumer[T, R, E]):
    _handler: EndpointHandler[HandlerState, T, R, E]
    _sc: StreamContext[T, R, E]
    _has_result: bool
    _pending: Optional[RotatingMap[str, ResultContext[HandlerState, T, R, E]]]
    _kafka_consumer: Optional[AIOKafkaConsumer]
    _runner_task: Optional[asyncio.Task]
    _stopped: bool
    _semaphore: Optional[asyncio.Semaphore]
    _active_count: int
    _active_lock: asyncio.Lock
    _active_event: asyncio.Event
    _tracer: Optional[Tracer]

    def __init__(
        self,
        endpoint: _AIOKafkaEndpoint,
        stream: TypedInputStream[T, R, E],
        handler: EndpointHandler[HandlerState, T, R, E],
        tracer: Optional[Tracer],
    ):
        super().__init__(endpoint=endpoint, input_stream=stream)
        self._handler = handler
        self._has_result = stream.get_result_stream() is not None
        self._pending = None
        self._kafka_consumer = None
        self._runner_task = None
        self._stopped = False
        self._semaphore = None
        self._active_count = 0
        self._active_lock = asyncio.Lock()
        self._active_event = asyncio.Event()
        self._active_event.set()
        self._tracer = tracer

        self._sc = StreamContext[T, R, E](
            stream=stream,
            result_stream=stream.get_result_stream(),
            collect=CollectFunc[T](stream.consume),
            error_collect=CollectFunc[E](stream.error_stream.consume),
        )

        if self._has_result:
            stream.set_result_consumer(_ResultConsumerProxy[R](self))  # type: ignore[arg-type]

        endpoint._consumer_obj = self
        endpoint.add_endpoint_consumer(self)

    async def start(self, ctx: Context) -> None:
        if self._has_result:
            self._pending = RotatingMap[str, Any](_PENDING_ROTATION_INTERVAL)
            await self._pending.start(ctx)

        cfg_ep = self.endpoint.config
        cfg_ds = self.endpoint.environment.config.get_data_connector_by_id(cfg_ep.id_data_connector)

        brokers = getattr(cfg_ds, 'brokers', None) or 'localhost:9092'
        topic = getattr(cfg_ep, 'topic', None)
        group_id = getattr(cfg_ep, 'consumer_group', None) or 'default'

        if not topic:
            raise ValueError(f"No topic configured for Kafka endpoint '{self.endpoint.name}'")

        kafka_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )
        await kafka_consumer.start()
        self._kafka_consumer = kafka_consumer
        self._runner_task = asyncio.create_task(self._consume_loop())

    async def stop(self, ctx: Context) -> None:
        self._stopped = True
        if self._runner_task is not None:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        if self._kafka_consumer is not None:
            await self._kafka_consumer.stop()
        if self._pending is not None:
            await self._pending.stop(ctx)

    async def _consume_loop(self) -> None:
        assert self._kafka_consumer is not None
        try:
            async for record in self._kafka_consumer:
                if self._stopped:
                    break
                await self._process_record(record)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._input_stream.environment.log.warn(
                f"Kafka consumer loop error for endpoint '{self.endpoint.name}': {e}"
            )

    async def _process_record(self, record: ConsumerRecord) -> None:
        limit = self._handler.concurrency(self._sc)
        if limit == 0:
            asyncio.create_task(self._endpoint_request(record))
        else:
            if self._semaphore is None or (hasattr(self._semaphore, '_value') and
                                            self._semaphore._value != limit):
                self._semaphore = asyncio.Semaphore(limit)
            await self._semaphore.acquire()
            semaphore = self._semaphore

            async def _run() -> None:
                try:
                    await self._endpoint_request(record)
                finally:
                    semaphore.release()

            asyncio.create_task(_run())

    async def _endpoint_request(self, record: ConsumerRecord) -> None:
        assert self._kafka_consumer is not None
        sid = new_stream_id()
        with_stream_id(sid)

        ep = cast(DataSourceEndpoint, self._endpoint)
        _, span = start_span(
            self._tracer if sampling_enabled() else None,
            "kafka.input",
            string_attr("stream", self._input_stream.name),
            string_attr("endpoint", ep.name),
        )
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                msg = ConsumerMessage(record, self._kafka_consumer)
                try:
                    handler_state = await self._handler.begin_request(self._sc)
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    return
                span_event(span, "begin_request")

                result: ResultContext[HandlerState, T, R, E] = ResultContext(handler_state)
                result._span = span
                if self._has_result and self._pending is not None:
                    self._pending.set(sid, result)
                    ep.on_pending_add(sid)

                try:
                    await self._handler.consume_message(self._sc, handler_state, msg, result)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    if self._has_result and self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                    await self._handler.end_request(self._sc, err, handler_state)
                    return
                span_event(span, "consume_message")

                if not self._has_result:
                    await self._handler.end_request(self._sc, None, handler_state)
                    return

                try:
                    await result._done.wait()
                    span_event(span, "done_received")
                except asyncio.CancelledError:
                    span_event(span, "context_cancelled")
                finally:
                    if self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)

                await self._handler.end_request(self._sc, None, handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()

    async def _consume_result(self, value: R) -> None:
        if not self._has_result or self._pending is None:
            return
        ep = cast(DataSourceEndpoint, self._endpoint)
        sid = stream_id_from_context()
        if sid is None:
            ep.on_missing_stream_id()
            return
        result, found = self._pending.get(sid)
        if not found or result is None:
            ep.on_late_result(sid)
            return

        message_id = self._handler.get_message_id(self._sc, result._handler_state, value)

        async with result._cb_lock:
            cb = result._callbacks.pop(message_id, None)
            if cb is None:
                ep.on_duplicate_message_id(sid, message_id)
                span_event(result._span, "duplicate_message_id",
                           string_attr("message_id", message_id))
                return
            remove = cb(self._sc, result._handler_state, value)
            if not remove:
                result._callbacks[message_id] = cb


def _make_tracer(stream: TypedInputStream, env: ServiceExecutionEnvironment) -> Optional[Tracer]:
    tracing = env.tracing
    if tracing is None:
        return None
    return tracing.tracer(env.service_config.name)


def make_aiokafka_endpoint_consumer[HandlerState, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, T, R, E]",
) -> Consumer[T]:
    """
    Creates an aiokafka datasource endpoint consumer.
    Equivalent to Go's MakeSaramaKafkaEndpointConsumer (datasource/kafka).
    """
    env = stream.environment
    cfg_ep = env.config.get_endpoint_config_by_id(stream.endpoint_id)

    datasource = env.get_datasource(cfg_ep.id_data_connector)
    if datasource is None:
        cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
        datasource = _AIOKafkaDataSource(connector_id=cfg_ds.id, env=env)
        env.add_datasource(datasource)
    ds = cast(_AIOKafkaDataSource, datasource)

    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is None:
        endpoint = _AIOKafkaEndpoint(datasource=ds, id_endpoint=stream.endpoint_id)
        ds.add_endpoint(endpoint)

    return _AIOKafkaTypedEndpointConsumer[HandlerState, T, R, E](
        endpoint=cast(_AIOKafkaEndpoint, endpoint),
        stream=stream,
        handler=handler,
        tracer=_make_tracer(stream, env),
    )

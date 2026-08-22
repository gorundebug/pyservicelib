#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Optional, Protocol, Any, Callable, cast

from aiokafka import AIOKafkaConsumer  # type: ignore[import-not-found,import-untyped]
from aiokafka.admin import (  # type: ignore[import-not-found,import-untyped]
    AIOKafkaAdminClient,
    NewTopic,
)
from aiokafka.errors import (  # type: ignore[import-not-found,import-untyped]
    TopicAlreadyExistsError,
    for_code,
)
from aiokafka.structs import (  # type: ignore[import-not-found,import-untyped]
    ConsumerRecord,
    TopicPartition,
)

from ...runtime.common import (
    TypedInputStream, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.environment.tracing import (
    Tracer, Span, start_endpoint_span, span_event, span_error, string_attr,
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

    def __init__(
        self,
        record: ConsumerRecord,
        consumer: AIOKafkaConsumer,
        mark_message: "Callable[[ConsumerRecord, str], None]",
    ):
        self._record = record
        self._consumer = consumer
        self.key = record.key
        self.value = record.value
        self.topic = record.topic
        self.partition = record.partition
        self.offset = record.offset
        self._mark_message = mark_message

    async def commit(self) -> None:
        """Manually commit this message's offset."""
        from aiokafka import TopicPartition  # type: ignore[import-untyped]
        tp = TopicPartition(self.topic, self.partition)
        await self._consumer.commit({tp: self.offset + 1})

    def mark_message(self, metadata: str = "") -> None:
        """Mark this offset for the managed periodic commit, as Sarama does."""
        self._mark_message(self._record, metadata)


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

    __slots__ = ("_handler_state", "_done", "_callbacks", "_span", "_once")

    _handler_state: HandlerState
    _done: asyncio.Future[None]
    _callbacks: dict[str, Any]
    _span: Optional[Span]
    _once: bool

    def __init__(self, handler_state: HandlerState):
        self._handler_state = handler_state
        self._done = asyncio.get_running_loop().create_future()
        self._callbacks = {}
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
        if not self._done.done():
            self._done.set_result(None)


class _NoopResultContext:
    __slots__ = ()

    def set_result_callback(self, message_id: str, cb: Any) -> None:
        del message_id, cb

    def done(self) -> None:
        pass


_NOOP_RESULT_CONTEXT = _NoopResultContext()


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
        enabled_configs = [
            ep.config
            for ep in self.endpoints
            if bool(getattr(ep.config, "enabled", False))
        ]
        if not enabled_configs:
            return
        cfg_ds = self.data_connector
        brokers = getattr(cfg_ds, "brokers", None) or ""
        if not brokers:
            raise ValueError(f"No brokers configured for Kafka data source '{self.name}'")
        await _create_topics(brokers, enabled_configs, cfg_ds)
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
    _auto_commit_task: Optional[asyncio.Task]
    _stopped: bool
    _active_count: int
    _concurrency_changed: asyncio.Condition
    _message_tasks: set[asyncio.Task[None]]
    _partition_locks: dict[tuple[str, int], asyncio.Lock]
    _marked_offsets: dict[TopicPartition, int]
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
        self._auto_commit_task = None
        self._stopped = False
        self._active_count = 0
        self._concurrency_changed = asyncio.Condition()
        self._message_tasks = set()
        self._partition_locks = {}
        self._marked_offsets = {}
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
        cfg_ep = self.endpoint.config
        if not cast(Any, cfg_ep).enabled:
            return

        self._stopped = False

        if self._has_result:
            self._pending = RotatingMap[str, Any](_PENDING_ROTATION_INTERVAL)
            await self._pending.start(ctx)

        cfg_ds = self.endpoint.environment.config.get_data_connector_by_id(cfg_ep.id_data_connector)

        brokers = getattr(cfg_ds, 'brokers', None) or ''
        if not brokers:
            raise ValueError(
                f"No brokers configured for Kafka data source '{self.endpoint.name}'"
            )
        topic = getattr(cfg_ep, 'topic', None)
        group_id = getattr(cfg_ep, 'consumer_group', None) or ''

        if not topic:
            raise ValueError(f"No topic configured for Kafka endpoint '{self.endpoint.name}'")
        if not group_id.strip():
            raise ValueError(
                f"No consumer group configured for Kafka endpoint '{self.endpoint.name}'"
            )

        kafka_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            **_client_options(cfg_ds),
        )
        await kafka_consumer.start()
        self._kafka_consumer = kafka_consumer
        self._runner_task = asyncio.create_task(self._consume_loop())
        self._auto_commit_task = asyncio.create_task(self._auto_commit_loop())

    async def stop(self, ctx: Context) -> None:
        async with self._concurrency_changed:
            self._stopped = True
            self._concurrency_changed.notify_all()
        if self._runner_task is not None:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        tasks = tuple(self._message_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._auto_commit_task is not None:
            self._auto_commit_task.cancel()
            try:
                await self._auto_commit_task
            except asyncio.CancelledError:
                pass
        if self._kafka_consumer is not None:
            try:
                await self._flush_marked_offsets()
            finally:
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

    async def _auto_commit_loop(self) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(1.0)
                try:
                    await self._flush_marked_offsets()
                except Exception as err:
                    self._input_stream.environment.log.warn(
                        f"Kafka offset commit failed for endpoint "
                        f"'{self.endpoint.name}': {err}"
                    )
        except asyncio.CancelledError:
            pass

    def _mark_message(self, record: ConsumerRecord, metadata: str) -> None:
        del metadata  # aiokafka's public commit API does not preserve metadata.
        partition = TopicPartition(record.topic, record.partition)
        offset = record.offset + 1
        self._marked_offsets[partition] = max(
            offset, self._marked_offsets.get(partition, offset)
        )

    async def _flush_marked_offsets(self) -> None:
        if self._kafka_consumer is None or not self._marked_offsets:
            return
        offsets = dict(self._marked_offsets)
        await self._kafka_consumer.commit(offsets)
        for partition, offset in offsets.items():
            if self._marked_offsets.get(partition, 0) <= offset:
                self._marked_offsets.pop(partition, None)

    async def _process_record(self, record: ConsumerRecord) -> None:
        async def _run() -> None:
            lane = self._partition_locks.setdefault(
                (record.topic, record.partition), asyncio.Lock()
            )
            async with lane:
                if not await self._acquire_concurrency():
                    return
                try:
                    await self._endpoint_request(record)
                finally:
                    await self._release_concurrency()

        task = asyncio.create_task(_run())
        self._message_tasks.add(task)
        task.add_done_callback(self._message_tasks.discard)

    async def _acquire_concurrency(self) -> bool:
        async with self._concurrency_changed:
            while True:
                if self._stopped:
                    return False
                limit = self._handler.concurrency(self._sc)
                if limit == 0 or self._active_count < limit:
                    self._active_count += 1
                    return True
                await self._concurrency_changed.wait()

    async def _release_concurrency(self) -> None:
        async with self._concurrency_changed:
            self._active_count -= 1
            self._concurrency_changed.notify_all()

    async def _endpoint_request(self, record: ConsumerRecord) -> None:
        assert self._kafka_consumer is not None
        sid = new_stream_id()
        with_stream_id(sid)

        ep = cast(DataSourceEndpoint, self._endpoint)
        _, span = start_endpoint_span(
            self._tracer,
            "kafka.input",
            self._input_stream.name,
            ep.name,
        )
        start_time: Optional[float] = None
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                msg = ConsumerMessage(
                    record, self._kafka_consumer, self._mark_message
                )
                try:
                    handler_state = await self._handler.begin_request(self._sc)
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    return
                span_event(span, "begin_request")
                start_time = ep.on_request_start()

                result: Optional[ResultContext[HandlerState, T, R, E]] = None
                result_ctx: Any
                if self._has_result:
                    result = ResultContext(handler_state)
                    result._span = span
                    result_ctx = result
                else:
                    result_ctx = _NOOP_RESULT_CONTEXT
                if result is not None and self._pending is not None:
                    self._pending.set(sid, result)
                    ep.on_pending_add(sid)

                try:
                    await self._handler.consume_message(self._sc, handler_state, msg, result_ctx)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    if self._has_result and self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                        await self._handler.end_request(
                            self._sc, err, handler_state
                        )
                    else:
                        await self._handler.end_request(
                            self._sc, err, handler_state
                        )
                    return
                span_event(span, "consume_message")

                if not self._has_result:
                    await self._handler.end_request(self._sc, None, handler_state)
                    return

                if result is None:
                    raise RuntimeError("Kafka result context was not initialized")

                try:
                    await asyncio.shield(result._done)
                    span_event(span, "done_received")
                except asyncio.CancelledError:
                    if result._done.done() and not result._done.cancelled():
                        span_event(span, "done_received")
                    else:
                        span_event(span, "context_cancelled")
                        end_err = RuntimeError("Kafka endpoint request cancelled")
                        span_error(span, end_err)
                if self._pending is not None:
                    self._pending.pop(sid)
                    ep.on_pending_remove(sid)
                await self._handler.end_request(
                    self._sc, end_err, handler_state
                )
        finally:
            if start_time is not None:
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

        message_id = self._handler.get_message_id(
            self._sc, result._handler_state, value
        )
        cb = result._callbacks.get(message_id)
        if cb is None:
            ep.on_unknown_message_id(sid, message_id)
            span_event(
                result._span, "unknown_message_id",
                string_attr("message_id", message_id),
            )
            return
        remove = cb(self._sc, result._handler_state, value)
        if remove and result._callbacks.pop(message_id, None) is None:
            ep.on_duplicate_message_id(sid, message_id)
            span_event(
                result._span, "duplicate_message_id",
                string_attr("message_id", message_id),
            )
        span_event(
            result._span, "result_consumed",
            string_attr("message_id", message_id),
        )


def _make_tracer(stream: TypedInputStream, env: ServiceExecutionEnvironment) -> Optional[Tracer]:
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

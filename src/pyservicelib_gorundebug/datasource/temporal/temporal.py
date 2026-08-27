#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Activate existing Python input graphs from Temporal Activities."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol, cast

from ...runtime.common import (
    CollectFunc,
    Consumer,
    DataSource,
    RuntimeEndpointConsumer,
    ServiceExecutionEnvironment,
    StreamContext,
    TypedInputStream,
)
from ...runtime.context import (
    Context,
    request_cancelled,
    request_deadline,
    request_priority,
    request_stream_id,
)
from ...runtime.datasource import DataSourceEndpoint, DataSourceEndpointConsumer, InputDataSource
from ...runtime.environment.tracing import (
    Tracer,
    Tracing,
    data_source_endpoint_tracing_enabled,
    sampling_enabled,
    sampling_scope,
    span_error,
    start_endpoint_span,
)
from ...runtime.durable_context import bind_durable_call_span
from ...runtime.schedule import (
    ScheduleBackend,
    ScheduleEndpointFunction,
    ScheduleTrigger,
    new_schedule_trigger,
)
from .connector import Connector, EndpointEnvelope, EndpointResult, make_connector


class _TemporalDataSource(InputDataSource):
    async def start(self, ctx: Context) -> None:
        del ctx

    async def stop(self, ctx: Context) -> None:
        del ctx


class _ResultConsumer[R](Consumer[R]):
    def __init__(self, owner: "_TemporalEndpointConsumer[Any, Any, R, Any]") -> None:
        self._owner = owner

    async def consume(self, value: R) -> None:
        self._owner.consume_result(value)


class EndpointHandler[HandlerState, T, R, E](Protocol):
    """User-owned lifecycle invoked before a Temporal value enters the graph."""

    async def begin_request(
        self, ctx: Context, sc: StreamContext[T, R, E]
    ) -> tuple[Context, HandlerState]: ...

    async def consume_message(
        self,
        ctx: Context,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: T,
    ) -> None: ...

    async def end_request(
        self,
        ctx: Context,
        sc: StreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


class _TemporalEndpointConsumer[Input, T, R, E](
    DataSourceEndpointConsumer[T, R, E], RuntimeEndpointConsumer
):
    def __init__(
        self,
        endpoint: DataSourceEndpoint,
        stream: TypedInputStream[T, R, E],
        connector: Connector,
        decode: Callable[[EndpointEnvelope], Input],
        invoke: Callable[[Input], Awaitable[None]],
    ) -> None:
        super().__init__(endpoint, stream)
        self._connector = connector
        self._decode = decode
        self._invoke = invoke
        tracing = stream.environment.tracing
        self._tracing: Optional[Tracing] = tracing
        self._tracer: Optional[Tracer] = (
            tracing.tracer(stream.environment.service_config.name)
            if tracing is not None
            else None
        )
        self._pending: dict[str, asyncio.Future[R]] = {}
        self._result_stream = stream.get_result_stream()
        if self._result_stream is not None:
            stream.set_result_consumer(_ResultConsumer(self))

    @property
    def id(self) -> int:
        return self.endpoint.id

    def consume_result(self, value: R) -> None:
        stream_id = request_stream_id.get()
        if not stream_id:
            self.endpoint.on_missing_stream_id()
            return
        pending = self._pending.get(stream_id)
        if pending is None:
            self.endpoint.on_late_result(stream_id)
            return
        if pending.done():
            self.endpoint.on_duplicate_message_id(stream_id, stream_id)
            return
        pending.set_result(value)

    async def activate(self, envelope: EndpointEnvelope) -> EndpointResult:
        value = self._decode(envelope)
        deadline = (
            datetime.fromtimestamp(
                envelope.deadline_unix_nano / 1_000_000_000,
                tz=timezone.utc,
            )
            if envelope.deadline_unix_nano > 0
            else None
        )
        stream_token = request_stream_id.set(envelope.stream_id or None)
        priority_token = request_priority.set(envelope.priority)
        deadline_token = request_deadline.set(deadline)
        cancelled = asyncio.Event()
        cancelled_token = request_cancelled.set(cancelled)
        started = self.endpoint.on_request_start()
        error: Optional[Exception] = None
        future: Optional[asyncio.Future[R]] = None
        durable_span = False
        try:
            with ExitStack() as scopes:
                scopes.enter_context(
                    sampling_scope(
                        sampling_enabled()
                        or data_source_endpoint_tracing_enabled(
                            self.endpoint.environment, self.endpoint.id,
                        )
                    )
                )
                _, span = start_endpoint_span(
                    self._tracer,
                    "temporal.input",
                    self.stream.name,
                    self.endpoint.name,
                )
                durable_span = bind_durable_call_span(span)
                with span.scoped():
                    try:
                        if self._result_stream is not None:
                            if envelope.stream_id in self._pending:
                                raise RuntimeError(
                                    f"Temporal endpoint {self.endpoint.name!r} already "
                                    f"has active execution {envelope.stream_id!r}"
                                )
                            future = asyncio.get_running_loop().create_future()
                            self._pending[envelope.stream_id] = future
                            self.endpoint.on_pending_add(envelope.stream_id)
                        await self._invoke(value)
                        if future is None:
                            return EndpointResult()
                        result = await future
                        result_stream = self._result_stream
                        if result_stream is None:
                            raise RuntimeError("Temporal endpoint result stream disappeared")
                        return EndpointResult(
                            bytes(result_stream.serde.serialize(result))
                        )
                    except Exception as exc:
                        span_error(span, exc)
                        raise
        except asyncio.CancelledError:
            cancelled.set()
            raise
        except Exception as exc:
            error = exc
            raise
        finally:
            if future is not None:
                self._pending.pop(envelope.stream_id, None)
                self.endpoint.on_pending_remove(envelope.stream_id)
            if not durable_span:
                span.end()
            self.endpoint.on_request_end(started, error)
            request_cancelled.reset(cancelled_token)
            request_deadline.reset(deadline_token)
            request_priority.reset(priority_token)
            request_stream_id.reset(stream_token)


def _get_or_create_datasource(
    connector_id: int,
    environment: ServiceExecutionEnvironment,
) -> tuple[DataSource, Connector]:
    connector = make_connector(connector_id, environment)
    existing = environment.get_datasource(connector_id)
    if existing is not None:
        return existing, connector
    datasource = _TemporalDataSource(connector_id, environment)
    environment.add_datasource(datasource)
    return datasource, connector


def _make_endpoint_consumer[Input, T, R, E](
    stream: TypedInputStream[T, R, E],
    decode: Callable[[EndpointEnvelope], Input],
    invoke: Callable[[Input], Awaitable[None]],
    workflow_class: type[Any] | None = None,
) -> Consumer[T]:
    environment = stream.environment
    cfg = environment.config.get_endpoint_config_by_id(stream.endpoint_id)
    datasource, connector = _get_or_create_datasource(
        cfg.id_data_connector, environment
    )
    if datasource.get_endpoint(cfg.id) is not None:
        raise ValueError(f"Temporal source endpoint {cfg.name!r} already exists")
    endpoint = DataSourceEndpoint(datasource, cfg.id)
    datasource.add_endpoint(endpoint)
    consumer = _TemporalEndpointConsumer(
        endpoint, stream, connector, decode, invoke
    )
    endpoint.add_endpoint_consumer(consumer)
    connector.register_endpoint(
        cfg.id,
        consumer.activate,
        lambda value: bytes(stream.serde.serialize(cast(T, value))),
        workflow_class,
    )
    environment.runtime.register_endpoint_consumer(consumer)
    return consumer


def make_direct_endpoint_consumer[T, R, E](
    stream: TypedInputStream[T, R, E],
    workflow_class: type[Any] | None = None,
) -> Consumer[T]:
    """Register a direct Activity-to-existing-input adapter."""

    return _make_endpoint_consumer(
        stream,
        lambda envelope: stream.serde.deserialize(envelope.payload),
        stream.consume,
        workflow_class,
    )


def make_direct_endpoint_consumer_with_handler[HandlerState, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: EndpointHandler[HandlerState, T, R, E],
    workflow_class: type[Any] | None = None,
) -> Consumer[T]:
    """Register a direct Temporal adapter through the user endpoint lifecycle."""

    sc = StreamContext[T, R, E](
        stream=stream,
        result_stream=stream.get_result_stream(),
        collect=CollectFunc[T](stream.consume),
        error_collect=CollectFunc[E](stream.error_stream.consume),
    )

    async def invoke(value: T) -> None:
        deadline = request_deadline.get()
        timeout = None
        if deadline is not None:
            timeout = timedelta(
                seconds=max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
            )
        ctx = Context(timeout)
        handler_ctx, handler_state = await handler.begin_request(ctx, sc)
        error: Optional[Exception] = None
        try:
            await handler.consume_message(handler_ctx, sc, handler_state, value)
        except Exception as exc:
            error = exc
            raise
        finally:
            await handler.end_request(handler_ctx, sc, error, handler_state)

    return _make_endpoint_consumer(
        stream,
        lambda envelope: stream.serde.deserialize(envelope.payload),
        invoke,
        workflow_class,
    )


def make_schedule_endpoint_consumer[T, R, E](
    stream: TypedInputStream[T, R, E],
    function: ScheduleEndpointFunction[T],
    workflow_class: type[Any] | None = None,
) -> Consumer[T]:
    """Bind scheduled and on-demand activation to one typed endpoint."""

    def decode(envelope: EndpointEnvelope) -> tuple[bool, ScheduleTrigger | T]:
        if not envelope.scheduled:
            return False, stream.serde.deserialize(envelope.payload)
        if (
            not envelope.schedule_id
            or envelope.scheduled_at_unix_nano <= 0
            or envelope.fired_at_unix_nano <= 0
        ):
            raise ValueError(
                f"invalid Temporal schedule envelope for endpoint {envelope.endpoint_id}"
            )
        return (
            True,
            new_schedule_trigger(
                envelope.endpoint_id,
                envelope.schedule_id,
                datetime.fromtimestamp(
                    envelope.scheduled_at_unix_nano / 1_000_000_000,
                    tz=timezone.utc,
                ),
                datetime.fromtimestamp(
                    envelope.fired_at_unix_nano / 1_000_000_000,
                    tz=timezone.utc,
                ),
                ScheduleBackend.TEMPORAL,
            ),
        )

    out: CollectFunc[T] = CollectFunc(stream.consume)

    async def invoke(value: tuple[bool, ScheduleTrigger | T]) -> None:
        scheduled, payload = value
        if scheduled:
            await function.on_trigger(cast(ScheduleTrigger, payload), out)
            return
        await stream.consume(cast(T, payload))

    return _make_endpoint_consumer(stream, decode, invoke, workflow_class)

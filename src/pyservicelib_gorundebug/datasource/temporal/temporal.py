#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Activate existing Python input graphs from Temporal Activities."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack, nullcontext
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Optional

from ...runtime.common import (
    Consumer,
    DataSource,
    RuntimeEndpointConsumer,
    ServiceExecutionEnvironment,
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
    sampling_scope,
    span_error,
    start_endpoint_span,
)
from ...runtime.schedule import ScheduleBackend, ScheduleTrigger, new_schedule_trigger
from ...runtime.temporal import Connector, EndpointEnvelope, EndpointResult, make_connector


class _TemporalDataSource(InputDataSource):
    async def start(self, ctx: Context) -> None:
        del ctx

    async def stop(self, ctx: Context) -> None:
        del ctx


class _ResultConsumer[R](Consumer[R]):
    def __init__(self, owner: "_TemporalEndpointConsumer[Any, R, Any]") -> None:
        self._owner = owner

    async def consume(self, value: R) -> None:
        self._owner.consume_result(value)


class _TemporalEndpointConsumer[T, R, E](
    DataSourceEndpointConsumer[T, R, E], RuntimeEndpointConsumer
):
    def __init__(
        self,
        endpoint: DataSourceEndpoint,
        stream: TypedInputStream[T, R, E],
        connector: Connector,
        decode: Callable[[EndpointEnvelope], T],
    ) -> None:
        super().__init__(endpoint, stream)
        self._connector = connector
        self._decode = decode
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
        try:
            with ExitStack() as scopes:
                remote_sampled = scopes.enter_context(
                    self._tracing.extract(envelope.trace_carrier)
                    if self._tracing is not None and envelope.trace_carrier
                    else nullcontext(False)
                )
                scopes.enter_context(
                    sampling_scope(envelope.sampling_enabled or remote_sampled)
                )
                _, span = start_endpoint_span(
                    self._tracer,
                    "temporal.input",
                    self.stream.name,
                    self.endpoint.name,
                )
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
                        await self.stream.consume(value)
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


def _make_endpoint_consumer[T, R, E](
    stream: TypedInputStream[T, R, E],
    decode: Callable[[EndpointEnvelope], T],
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
    consumer = _TemporalEndpointConsumer(endpoint, stream, connector, decode)
    endpoint.add_endpoint_consumer(consumer)
    connector.register_endpoint(cfg.id, consumer.activate)
    environment.runtime.register_endpoint_consumer(consumer)
    return consumer


def make_direct_endpoint_consumer[T, R, E](
    stream: TypedInputStream[T, R, E],
) -> Consumer[T]:
    """Register a direct Activity-to-existing-input adapter."""

    return _make_endpoint_consumer(
        stream,
        lambda envelope: stream.serde.deserialize(envelope.payload),
    )


def make_schedule_endpoint_consumer[R, E](
    stream: TypedInputStream[ScheduleTrigger, R, E],
) -> Consumer[ScheduleTrigger]:
    """Register a Temporal Schedule that emits the shared ScheduleTrigger."""

    def decode(envelope: EndpointEnvelope) -> ScheduleTrigger:
        if (
            not envelope.scheduled
            or not envelope.schedule_id
            or envelope.scheduled_at_unix_nano <= 0
            or envelope.fired_at_unix_nano <= 0
        ):
            raise ValueError(
                f"invalid Temporal schedule envelope for endpoint {envelope.endpoint_id}"
            )
        return new_schedule_trigger(
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
        )

    return _make_endpoint_consumer(stream, decode)

#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Submit ordinary sink values to symmetric Temporal endpoints."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timezone
from typing import Optional

from ...runtime.common import (
    Consumer,
    DataSink,
    OutputEndpointConsumer,
    RuntimeEndpointConsumer,
    ServiceExecutionEnvironment,
    TypedSinkStream,
    TypedSinkStreamWithResult,
)
from ...runtime.context import (
    Context,
    new_stream_id,
    priority_from_context,
    request_deadline,
    stream_id_from_context,
)
from ...runtime.datasink import DataSinkEndpoint, OutputDataSink
from ...runtime.environment.tracing import sampling_enabled
from ...runtime.temporal import Connector, EndpointEnvelope, make_connector
from ...runtime.serde import TypedStreamSerde


class _TemporalDataSink(OutputDataSink):
    async def start(self, ctx: Context) -> None:
        del ctx

    async def stop(self, ctx: Context) -> None:
        del ctx


class _TemporalSinkConsumer[T, R, E](
    Consumer[T], OutputEndpointConsumer, RuntimeEndpointConsumer
):
    def __init__(
        self,
        endpoint: DataSinkEndpoint,
        connector: Connector,
        input_serde: TypedStreamSerde[T],
        result_serde: Optional[TypedStreamSerde[R]],
        emit_result: Optional[Callable[[R], Awaitable[None]]],
    ) -> None:
        self._endpoint = endpoint
        self._connector = connector
        self._input_serde = input_serde
        self._result_serde = result_serde
        self._emit_result = emit_result

    @property
    def endpoint(self) -> DataSinkEndpoint:
        return self._endpoint

    @property
    def id(self) -> int:
        return self.endpoint.id

    async def consume(self, value: T) -> None:
        started = self.endpoint.on_request_start()
        error: Optional[Exception] = None
        try:
            execution_id = stream_id_from_context() or new_stream_id()
            stream_id = stream_id_from_context() or execution_id
            priority = priority_from_context()
            deadline = request_deadline.get()
            deadline_nanos = 0
            if deadline is not None:
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                deadline_nanos = int(deadline.timestamp() * 1_000_000_000)
            result = await self._connector.submit_endpoint(
                self.endpoint.id,
                EndpointEnvelope(
                    version=1,
                    endpoint_id=self.endpoint.id,
                    execution_id=execution_id,
                    stream_id=stream_id,
                    priority=priority if priority is not None else 0,
                    deadline_unix_nano=deadline_nanos,
                    sampling_enabled=sampling_enabled(),
                    payload=bytes(self._input_serde.serialize(value)),
                ),
                self._result_serde is not None,
            )
            if self._result_serde is not None:
                result_value = self._result_serde.deserialize(result.payload)
                if self._emit_result is None:
                    raise RuntimeError("Temporal result consumer is not configured")
                await self._emit_result(result_value)
        except Exception as exc:
            error = exc
            raise
        finally:
            self.endpoint.on_request_end(started, error)


def _get_or_create_datasink(
    connector_id: int,
    environment: ServiceExecutionEnvironment,
) -> tuple[DataSink, Connector]:
    connector = make_connector(connector_id, environment)
    existing = environment.get_datasink(connector_id)
    if existing is not None:
        return existing, connector
    datasink = _TemporalDataSink(connector_id, environment)
    environment.add_datasink(datasink)
    return datasink, connector


def _create_endpoint(
    stream: TypedSinkStream[object, object]
    | TypedSinkStreamWithResult[object, object, object],
) -> tuple[DataSinkEndpoint, Connector]:
    environment = stream.environment
    cfg = environment.config.get_endpoint_config_by_id(stream.endpoint_id)
    datasink, connector = _get_or_create_datasink(cfg.id_data_connector, environment)
    if datasink.get_endpoint(cfg.id) is not None:
        raise ValueError(f"Temporal sink endpoint {cfg.name!r} already exists")
    endpoint = DataSinkEndpoint(datasink, cfg.id)
    datasink.add_endpoint(endpoint)
    connector.register_endpoint_submission(cfg.id)
    return endpoint, connector


def make_direct_endpoint_consumer[T, E](
    stream: TypedSinkStream[T, E],
) -> Consumer[T]:
    """Create a submission-only Temporal endpoint sink."""

    endpoint, connector = _create_endpoint(stream)  # type: ignore[arg-type]
    consumer = _TemporalSinkConsumer[T, object, E](
        endpoint, connector, stream.serde, None, None
    )
    endpoint.add_endpoint_consumer(consumer)
    stream.set_sink_consumer(consumer)
    stream.environment.runtime.register_endpoint_consumer(consumer)
    return consumer


def make_direct_endpoint_consumer_with_result[T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
) -> Consumer[T]:
    """Create a Temporal sink that emits the existing endpoint result."""

    endpoint, connector = _create_endpoint(stream)  # type: ignore[arg-type]
    consumer = _TemporalSinkConsumer[T, R, E](
        endpoint,
        connector,
        stream.input_serde,
        stream.serde,
        stream.consume_result,
    )
    endpoint.add_endpoint_consumer(consumer)
    stream.set_sink_consumer(consumer)
    stream.environment.runtime.register_endpoint_consumer(consumer)
    return consumer

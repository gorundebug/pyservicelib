#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import (
    TypedQueryStream, TypedConsumedStream, TypedStream,
    Consumer, StreamConsumer, RuntimeHelpers,
)
from .error import ErrorStream
from ..runtime.config.stream_types import QueryStreamConfig
from ..runtime.environment.tracing import Tracer, sampling_enabled, start_stream_span
from ..runtime.serde.serde import StreamSerde, StubSerde, TypedStreamSerde


class QueryStream[T, R, E](TypedQueryStream[T, R, E]):
    _source: TypedStream[T]
    _query_consumer: Optional[Consumer[T]]
    _error_stream: ErrorStream[E]
    _consumer: Optional[StreamConsumer[R]]
    _endpoint_id: int
    _tracer: Optional[Tracer]

    def __init__(self, cfg: QueryStreamConfig, stream: TypedStream[T]):
        env = stream.environment
        value_type = cfg.value_type
        if value_type:
            serde = RuntimeHelpers[R](env).make_stream_serde(type_name=value_type)
        else:
            serde = StreamSerde(StubSerde('result'))
        super().__init__(stream_id=cfg.id, env=env, serde=serde)
        self._source = stream
        self._endpoint_id = cfg.id_endpoint
        self._query_consumer = None
        self._consumer = None
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
        env.runtime.register_stream(self)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    @property
    def consumer(self) -> Optional[StreamConsumer[R]]:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[R]) -> None:
        self._consumer = value
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)

    async def consume(self, value: T) -> None:
        endpoint = self.environment.config.get_endpoint_config_by_id(self._endpoint_id)
        if endpoint is not None and endpoint.enabled is False:
            return
        if self._tracer is None or not sampling_enabled():
            if self._query_consumer is not None:
                await self._query_consumer.consume(value)
            return
        _, span = start_stream_span(self._tracer, "stream.query", self)
        try:
            with span.scoped():
                if self._query_consumer is not None:
                    await self._query_consumer.consume(value)
        finally:
            span.end()

    async def consume_result(self, value: R) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    @property
    def input_serde(self) -> TypedStreamSerde[T]:
        return self._source.serde

    @property
    def error_stream(self) -> TypedConsumedStream[E]:
        return self._error_stream

    def set_query_consumer(self, consumer: Consumer[T]) -> None:
        self._query_consumer = consumer

    @property
    def endpoint_id(self) -> int:
        return self._endpoint_id

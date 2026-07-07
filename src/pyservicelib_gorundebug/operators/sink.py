#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import (
    TypedSinkStream, TypedSinkStreamWithResult, TypedConsumedStream, TypedStream,
    Consumer, StreamConsumer, ErrorStream, RuntimeHelpers,
)
from ..runtime.config.stream_types import SinkStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from ..runtime.serde.serde import StreamSerde, StubSerde


class SinkStream[T, E](TypedSinkStream[T, E]):
    _source: TypedStream[T]
    _sink_consumer: Optional[Consumer[T]]
    _error_stream: ErrorStream[E]
    _endpoint_id: int
    _tracer: Optional[Tracer]

    def __init__(self, cfg: SinkStreamConfig, stream: TypedStream[T]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env, serde=stream.serde)
        self._source = stream
        self._endpoint_id = cfg.id_endpoint
        self._sink_consumer = None
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.sink",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if self._sink_consumer is not None:
                    await self._sink_consumer.consume(value)
        finally:
            span.end()

    @property
    def error_stream(self) -> TypedConsumedStream[E]:
        return self._error_stream

    def set_sink_consumer(self, consumer: Consumer[T]) -> None:
        self._sink_consumer = consumer

    @property
    def endpoint_id(self) -> int:
        return self._endpoint_id


class SinkStreamWithResult[T, R, E](TypedSinkStreamWithResult[T, R, E]):
    _source: TypedStream[T]
    _sink_consumer: Optional[Consumer[T]]
    _error_stream: ErrorStream[E]
    _consumer: Optional[StreamConsumer[R]]
    _endpoint_id: int
    _tracer: Optional[Tracer]

    def __init__(self, cfg: SinkStreamConfig, stream: TypedStream[T]):
        env = stream.environment
        value_type = cfg.value_type
        if value_type:
            serde = RuntimeHelpers[R](env).make_stream_serde(type_name=value_type)
        else:
            serde = StreamSerde(StubSerde('result'))
        super().__init__(stream_id=cfg.id, env=env, serde=serde)
        self._source = stream
        self._endpoint_id = cfg.id_endpoint
        self._sink_consumer = None
        self._consumer = None
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
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
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.sink",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if self._sink_consumer is not None:
                    await self._sink_consumer.consume(value)
        finally:
            span.end()

    async def consume_result(self, value: R) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    @property
    def error_stream(self) -> TypedConsumedStream[E]:
        return self._error_stream

    def set_sink_consumer(self, consumer: Consumer[T]) -> None:
        self._sink_consumer = consumer

    @property
    def endpoint_id(self) -> int:
        return self._endpoint_id

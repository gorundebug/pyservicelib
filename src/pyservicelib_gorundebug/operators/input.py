#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import (
    ServiceExecutionEnvironment, TypedInputStream, TypedConsumedStream, TypedStream,
    Consumer, StreamConsumer, Stream, RuntimeHelpers, RuntimeKeyValueHelpers,
)
from .error import ErrorStream
from ..runtime.config.stream_types import InputStreamConfig
from ..runtime.datastruct import KeyValue
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from ..runtime.serde.serde import StreamSerde, StubSerde


class _ResultLink[T, R, E](StreamConsumer[R]):
    """Routes R results from upstream source back into InputStream.consume_result()."""

    def __init__(self, input_stream: "InputStream[T, R, E]"):
        self._input_stream = input_stream

    async def consume(self, value: R) -> None:
        await self._input_stream._consume_result(value)

    @property
    def stream(self) -> Stream:
        return self._input_stream


class InputStream[T, R, E](TypedInputStream[T, R, E]):
    _id_endpoint: int
    _error_stream: ErrorStream[E]
    _result_consumer: Optional[Consumer[R]]
    _result_link: Optional[_ResultLink[T, R, E]]
    _result_source: Optional[TypedStream[R]]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: InputStreamConfig, env: ServiceExecutionEnvironment):
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[T](env).make_stream_serde(type_name=cfg.value_type))
        self._id_endpoint = cfg.id_endpoint
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
        self._result_consumer = None
        self._result_link = None
        self._result_source = None
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None

    @property
    def endpoint_id(self) -> int:
        return self._id_endpoint

    @property
    def error_stream(self) -> TypedConsumedStream[E]:
        return self._error_stream

    def get_result_stream(self) -> Optional[TypedStream[R]]:
        return self._result_source

    def set_result_consumer(self, consumer: Consumer[R]) -> None:
        self._result_consumer = consumer

    def set_source(self, source: TypedStream[R]) -> None:
        link: _ResultLink[T, R, E] = _ResultLink(self)
        self._result_link = link
        self._result_source = source
        source.consumer = link

    async def _consume_result(self, value: R) -> None:
        if self._result_consumer is not None:
            await self._result_consumer.consume(value)

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.input",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if self._caller is not None:
                    await self._caller.consume(value)
        finally:
            span.end()


class _ResultLinkKV[K, V, R, E](StreamConsumer[R]):
    """Routes R results back into InputKVStream.consume_result()."""

    def __init__(self, input_stream: "InputKVStream[K, V, R, E]"):
        self._input_stream = input_stream

    async def consume(self, value: R) -> None:
        await self._input_stream._consume_result(value)

    @property
    def stream(self) -> Stream:
        return self._input_stream


class InputKVStream[K, V, R, E](TypedInputStream[KeyValue[K, V], R, E]):
    _id_endpoint: int
    _error_stream: ErrorStream[E]
    _result_consumer: Optional[Consumer[R]]
    _result_link: Optional[_ResultLinkKV[K, V, R, E]]
    _result_source: Optional[TypedStream[R]]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: InputStreamConfig, env: ServiceExecutionEnvironment):
        super().__init__(
            stream_id=cfg.id,
            env=env,
            serde=RuntimeKeyValueHelpers[K, V](env).make_key_value_stream_serde(
                key_type_name=cfg.value_type,
                value_type_name=cfg.value_type,
            ),
        )
        self._id_endpoint = cfg.id_endpoint
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
        self._result_consumer = None
        self._result_link = None
        self._result_source = None
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None

    @property
    def endpoint_id(self) -> int:
        return self._id_endpoint

    @property
    def error_stream(self) -> TypedConsumedStream[E]:
        return self._error_stream

    def get_result_stream(self) -> Optional[TypedStream[R]]:
        return self._result_source

    def set_result_consumer(self, consumer: Consumer[R]) -> None:
        self._result_consumer = consumer

    def set_source(self, source: TypedStream[R]) -> None:
        link: _ResultLinkKV[K, V, R, E] = _ResultLinkKV(self)
        self._result_link = link
        self._result_source = source
        source.consumer = link

    async def _consume_result(self, value: R) -> None:
        if self._result_consumer is not None:
            await self._result_consumer.consume(value)

    async def consume(self, value: KeyValue[K, V]) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.input",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if self._caller is not None:
                    await self._caller.consume(value)
        finally:
            span.end()

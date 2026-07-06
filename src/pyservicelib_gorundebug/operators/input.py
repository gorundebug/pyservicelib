#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Any, Optional

from ..runtime.common import (
    ServiceExecutionEnvironment, TypedInputStream, TypedConsumedStream, TypedStream,
    Consumer, StreamConsumer, Stream, ErrorStream, RuntimeHelpers, RuntimeKeyValueHelpers,
)
from ..runtime.config.stream_types import InputStreamConfig
from ..runtime.datastruct import KeyValue
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


class InputStream[T, R=Any, E=Any](TypedInputStream[T, R, E]):
    _id_endpoint: int
    _error_stream: ErrorStream[E]
    _result_consumer: Optional[Consumer[R]]
    _result_link: Optional[_ResultLink[T, R, E]]
    _result_source: Optional[TypedStream[R]]

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
        if self._caller is not None:
            await self._caller.consume(value)


class _ResultLinkKV[K, V, R, E](StreamConsumer[R]):
    """Routes R results back into InputKVStream.consume_result()."""

    def __init__(self, input_stream: "InputKVStream[K, V, R, E]"):
        self._input_stream = input_stream

    async def consume(self, value: R) -> None:
        await self._input_stream._consume_result(value)

    @property
    def stream(self) -> Stream:
        return self._input_stream


class InputKVStream[K, V, R=Any, E=Any](TypedInputStream[KeyValue[K, V], R, E]):
    _id_endpoint: int
    _error_stream: ErrorStream[E]
    _result_consumer: Optional[Consumer[R]]
    _result_link: Optional[_ResultLinkKV[K, V, R, E]]
    _result_source: Optional[TypedStream[R]]

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
        if self._caller is not None:
            await self._caller.consume(value)

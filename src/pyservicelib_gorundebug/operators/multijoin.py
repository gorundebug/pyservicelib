#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Any, cast, Optional
from datetime import timedelta
from abc import ABC

from ..runtime.common import (StreamFunction, Collect, Collector, Stream, StreamConsumer,
                              ServiceExecutionEnvironment, TypedStream, TypedMultiJoinConsumedStream, RuntimeHelpers)
from ..runtime.config import StreamConfig
from ..runtime.config.stream_types import MultiJoinStreamConfig
from ..runtime.datastruct import KeyValue
from ..runtime.environment.tracing import Tracer, sampling_enabled, start_stream_span
from ..runtime.serde import Serializer, BytesBuffer, StreamKeyValueSerde
from ..runtime.store import JoinStorageFactory, JoinStorage
from ..runtime.store.storage import JoinStorageConfig
from .functions import MultiJoinFunction


class _MultiJoinStorageConfig(JoinStorageConfig):
    __slots__ = ('_name', '_ttl', '_renew_ttl')

    def __init__(self, cfg: MultiJoinStreamConfig) -> None:
        self._name = cfg.name
        self._ttl = timedelta(milliseconds=cfg.ttl)
        self._renew_ttl = cfg.renew_ttl

    @property
    def name(self) -> str:
        return self._name

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    @property
    def renew_ttl(self) -> bool:
        return self._renew_ttl


class MultiJoinFunctionContext[K: Hashable, T, R](StreamFunction[R]):
    _fn: MultiJoinFunction[K, T, R]

    def __init__(self, stream: TypedStream[R], fn: MultiJoinFunction[K, T, R]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, key: K, values: list[list[Any]], out: Collect[R]) -> bool:
        self.before_call()
        result = await self._fn.multi_join(self._stream, key, values, out)
        self.after_call()
        return result


class MultiJoinStream[K: Hashable, T, R](TypedMultiJoinConsumedStream[K, T, R]):
    _source: TypedStream[KeyValue[K, T]]
    _f: MultiJoinFunctionContext[K, T, R]
    _join_storage: JoinStorage[K]
    _links: list["MultiJoinLinkStream"]
    _collector: Collector[R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: MultiJoinStreamConfig, stream: TypedStream[KeyValue[K, T]],
                 fn: MultiJoinFunction[K, T, R]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = MultiJoinFunctionContext[K, T, R](self, fn)
        self._links = []
        self._collector = Collector[R](None)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self
        self._join_storage = JoinStorageFactory[K]().make_storage(
            cfg.join_storage, env, _MultiJoinStorageConfig(cfg))
        self.environment.runtime.register_storage(self._join_storage)

    @property
    def consumer(self):
        return self._consumer

    @consumer.setter
    def consumer(self, value):
        self._consumer = value
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)
        self._collector = Collector(self._caller)

    async def _consume(self, key: K, index: int, value: Any):
        async def _join_callback(values: list[list[Any]]) -> bool:
            if len(values) > 0 and len(values[0]) > 0:
                return await self._f.call(key, values, self._collector)
            return False

        await self._join_storage.join_value(key, index, value, _join_callback)

    async def consume(self, value: KeyValue[K, T]) -> None:
        if self._tracer is None or not sampling_enabled():
            await self._consume(value.key, 0, value.value)
            return
        _, span = start_stream_span(self._tracer, "stream.join", self)
        try:
            with span.scoped():
                await self._consume(value.key, 0, value.value)
        finally:
            span.end()

    async def consume_right(self, index: int, value: KeyValue[K, Any]) -> None:
        await self._consume(value.key, index, value.value)

    def add_link(self, link: "MultiJoinLinkStream") -> int:
        index = len(self._links) + 1
        self._links.append(link)
        return index


class MultiJoinLinkStream(Stream, ABC):
    _serde: Serializer
    _index: int

    def __init__(self, serde: Serializer):
        self._index = 0
        self._serde = serde

    def serialize_value(self, value: Any) -> bytearray:
        return self._serde.serialize_obj(value, bytearray())

    def deserialize_value(self, b: BytesBuffer) -> Any:
        return self._serde.deserialize_obj(b)


class MultiJoinLink[K: Hashable, T1, T2, R](MultiJoinLinkStream, StreamConsumer[KeyValue[K, T2]]):
    _multi_join_stream: MultiJoinStream[K, T1, R]
    _source: TypedStream[KeyValue[K, T2]]

    def __init__(self, multi_join_stream: TypedMultiJoinConsumedStream[K, T1, R],
                 stream: TypedStream[KeyValue[K, T2]]):
        super().__init__(cast(StreamKeyValueSerde[K, T2], stream.serde).value_serializer)
        self._multi_join_stream = cast(MultiJoinStream[K, T1, R], multi_join_stream)
        self._source = stream
        stream.consumer = self
        self._index = self._multi_join_stream.add_link(self)

    @property
    def name(self) -> str:
        return self._multi_join_stream.name

    @property
    def transformation_name(self) -> str:
        return self._multi_join_stream.transformation_name

    @property
    def type_name(self) -> str:
        return self._multi_join_stream.type_name

    @property
    def id(self) -> int:
        return self._multi_join_stream.id

    @property
    def config(self) -> StreamConfig:
        return self._multi_join_stream.config

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._multi_join_stream.environment

    @property
    def consumers(self) -> list[Stream]:
        return self._multi_join_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._multi_join_stream

    async def consume(self, value: KeyValue[K, T2]) -> None:
        await self._multi_join_stream.consume_right(self._index, value)

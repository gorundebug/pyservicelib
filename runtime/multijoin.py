#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Any, cast
from abc import ABC


from pyservicelib.api.models.join_storage_type import JoinStorageType
from pyservicelib.runtime.common import StreamFunction, Collect, Stream, StreamConsumer
from pyservicelib.runtime.common import TypedStream, TypedMultiJoinConsumedStream, RuntimeHelpers
from pyservicelib.runtime.common import ServiceExecutionEnvironment
from pyservicelib.runtime.config import StreamConfig
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.functions import MultiJoinFunction
from pyservicelib.runtime.serde import Serializer, BytesBuffer, StreamKeyValueSerde
from pyservicelib.runtime.store import JoinStorageFactory, JoinStorage
from pyservicelib.runtime.serviceapp import JoinStreamStorageConfig


class MultiJoinFunctionContext[K: Hashable, T, R](StreamFunction[R]):
    _fn: MultiJoinFunction[K, T, R]

    def __init__(self, context: TypedStream[R], fn: MultiJoinFunction[K, T, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, key: K, values: list[list[Any]], out: Collect[R]):
        self.before_call()
        await self._fn.multi_join(self._context, key, values, out)
        self.after_call()


class MultiJoinStream[K: Hashable, T, R](TypedMultiJoinConsumedStream[K, T, R], Collect[R]):

    _source: TypedStream[KeyValue[K, T]]
    _f: MultiJoinFunctionContext[K, T, R]
    _join_storage: JoinStorage[K]
    _links: list["MultiJoinLinkStream"]

    def __init__(self, name: str, stream: TypedStream[KeyValue[K, T]],
                 fn: MultiJoinFunction[K, T, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"MultiJoinStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the MultiJoinStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=stream.environment,
                         serde=RuntimeHelpers[R](stream.environment).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = MultiJoinFunctionContext[K, T, R](self, fn)
        stream.consumer = self
        join_storage_type = JoinStorageType.HashMap if cfg.join_storage is None else cfg.join_storage
        self._join_storage = JoinStorageFactory[K]().make_storage(join_storage_type,
                                                                  stream.environment,
                                                                  JoinStreamStorageConfig(self))
        self.environment.runtime.register_storage(self._join_storage)


    async def _consume(self, key: K, index: int, value: Any):
        async def _join_callback(values: list[list[Any]]):
            if len(values) > 0 and len(values[0]) > 0:
                await self._f.call(key, values, self)

        await self._join_storage.join_value(key, index, value, _join_callback)

    async def consume(self, value: KeyValue[K, T]) -> None:
        await self._consume(value.key, 0, value.value)

    async def consume_right(self, index: int, value: KeyValue[K, Any]) -> None:
        await self._consume(value.key, index, value.value)

    async def out(self, value: R) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

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
        return self._multi_join_stream.stream

    async def consume(self, value: KeyValue[K, T2]) -> None:
        await self._multi_join_stream.consume_right(self._index, value)
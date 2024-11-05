#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Any

from pyservicelib.api.models.join_storage_type import JoinStorageType
from pyservicelib.api.models.join_type import JoinType
from pyservicelib.runtime.common import StreamFunction, Collect, Stream, StreamConsumer
from pyservicelib.runtime.common import TypedStream, TypedJoinConsumedStream, RuntimeHelpers
from pyservicelib.runtime.common import ServiceExecutionEnvironment
from pyservicelib.runtime.serviceapp import JoinStreamStorageConfig
from pyservicelib.runtime.config import StreamConfig
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.functions import JoinFunction
from pyservicelib.runtime.store import JoinStorageFactory, JoinStorage


class JoinFunctionContext[K: Hashable, T1, T2, R](StreamFunction[R]):
    _fn: JoinFunction[K, T1, T2, R]

    def __init__(self, context: TypedStream[R], fn: JoinFunction[K, T1, T2, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]):
        self.before_call()
        await self._fn.join(self._context, key, left_values, right_values, out)
        self.after_call()


class JoinStream[K: Hashable, T1, T2, R](TypedJoinConsumedStream[K, T1, T2, R], Collect[R]):

    _source: TypedStream[KeyValue[K, T1]]
    _f: JoinFunctionContext[K, T1, T2, R]
    _join_link: "JoinLink[K, T1, T2, R]"
    _join_storage: JoinStorage[K]
    _join_type: JoinType

    def __init__(self, name: str, stream: TypedStream[KeyValue[K, T1]],
                 right_stream: TypedStream[KeyValue[K, T2]],
                 fn: JoinFunction[K, T1, T2, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"JoinStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the JoinStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=stream.environment,
                         serde=RuntimeHelpers[R](stream.environment).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = JoinFunctionContext[K, T1, T2, R](self, fn)
        stream.consumer = self
        self._join_link = JoinLink[K, T1, T2, R](self, right_stream)
        join_storage_type = JoinStorageType.HashMap if cfg.join_storage is None else cfg.join_storage
        self._join_type = JoinType.Inner if cfg.join_type is None else cfg.join_type
        self._join_storage = JoinStorageFactory[K]().make_storage(join_storage_type,
                                                                  stream.environment,
                                                                  JoinStreamStorageConfig(self))
        self.environment.runtime.register_storage(self._join_storage)


    async def _consume(self, key: K, index: int, value: Any):
        async def _join_callback(values: list[list[Any]]):
            can_call = False
            if self._join_type == JoinType.Inner:
                can_call = len(values) > 1 and len(values[0]) != 0 and len(values[1]) != 0
            elif self._join_type == JoinType.Left:
                can_call = len(values) > 0 and len(values[0]) != 0
            elif self._join_type == JoinType.Right:
                can_call = len(values) > 1 and len(values[1]) != 0
            elif self._join_type == JoinType.Outer:
                can_call = True

            if can_call:
                await self._f.call(key, values[0], values[1], self)

        await self._join_storage.join_value(key, index, value, _join_callback)

    async def consume(self, value: KeyValue[K, T1]) -> None:
        await self._consume(value.key, 0, value.value)

    async def consume_right(self, value: KeyValue[K, T2]) -> None:
        await self._consume(value.key, 1, value.value)

    async def out(self, value: R) -> None:
        if self._caller is not None:
            await self._caller.consume(value)


class JoinLink[K: Hashable, T1, T2, R](Stream, StreamConsumer[KeyValue[K, T2]]):
    _join_stream: JoinStream[K, T1, T2, R]
    _source: TypedStream[KeyValue[K, T2]]

    def __init__(self, join_stream: JoinStream[K, T1, T2, R],
                 stream: TypedStream[KeyValue[K, T2]]):
        self._join_stream = join_stream
        self._source = stream
        stream.consumer = self

    @property
    def name(self) -> str:
        return self._join_stream.name

    @property
    def transformation_name(self) -> str:
        return self._join_stream.transformation_name

    @property
    def type_name(self) -> str:
        return self._join_stream.type_name

    @property
    def id(self) -> int:
        return self._join_stream.id

    @property
    def config(self) -> StreamConfig:
        return self._join_stream.config

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._join_stream.environment

    @property
    def consumers(self) -> list[Stream]:
        return self._join_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._join_stream.stream

    async def consume(self, value: KeyValue[K, T2]) -> None:
        await self._join_stream.consume_right(value)
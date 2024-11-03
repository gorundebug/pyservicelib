#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, List, Any

from pyservicelib.runtime.common import StreamFunction, TypedStreamSerde, Collect, Stream, StreamConsumer
from pyservicelib.runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from pyservicelib.runtime.common import ServiceExecutionEnvironment
from pyservicelib.runtime.config import StreamConfig
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.functions import JoinFunction


class JoinFunctionContext[K: Hashable, T1, T2, R](StreamFunction[R]):
    _fn: JoinFunction[K, T1, T2, R]

    def __init__(self, context: TypedStream[R], fn: JoinFunction[K, T1, T2, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, key: K, left_values: List[T1], right_values: List[T2], out: Collect[R]):
        self.before_call()
        await self._fn.join(self._context, key, left_values, right_values, out)
        self.after_call()


class JoinStream[K: Hashable, T1, T2, R](TypedTransformConsumedStream[KeyValue[K, T1], R]):
    _source: TypedStream[KeyValue[K, T1]]
    _serdeIn: TypedStreamSerde[KeyValue[K, T1]]
    _f: JoinFunctionContext[K, T1, T2, R]
    _join_link: "JoinLink[K, T1, T2, R]"

    def __init__(self, name: str, stream: TypedStream[KeyValue[K, T1]], fn: JoinFunction[K, T1, T2, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"JoinStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the JoinStream with name '{name}' is not defined")

        super().__init__(cfg=cfg,
                         serde=RuntimeHelpers[R](stream.environment).make_serde(type_name=cfg.value_type),
                         env=stream.environment)
        self._source = stream
        self._serdeIn = stream.serde
        self._f = JoinFunctionContext[K, T1, T2, R](self, fn)
        stream.consumer = self
        self._join_link = JoinLink[K, T1, T2, R](self)

    async def _consume(self, key: K, index: int, value: Any):
        pass

    async def consume(self, value: KeyValue[K, T1]) -> None:
        await self._consume(value.key, 0, value.value)

    async def consume_right(self, value: KeyValue[K, T2]) -> None:
        await self._consume(value.key, 1, value.value)


class JoinLink[K: Hashable, T1, T2, R](Stream, StreamConsumer[KeyValue[K, T2]]):
    _join_stream: JoinStream[K, T1, T2, R]

    def __init__(self, join_stream: JoinStream[K, T1, T2, R]):
        self._join_stream = join_stream

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
    def consumers(self) -> List[Stream]:
        return self._join_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._join_stream.stream

    async def consume(self, value: KeyValue[K, T2]) -> None:
        await self._join_stream.consume_right(value)
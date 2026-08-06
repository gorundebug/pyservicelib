#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Any, Optional
from datetime import timedelta

from ..runtime.common import (StreamFunction, Collect, Collector, Stream, StreamConsumer,
                              ServiceExecutionEnvironment, TypedStream, TypedJoinConsumedStream, RuntimeHelpers)
from ..runtime.config import StreamConfig
from ..runtime.config.stream_types import JoinStreamConfig
from ..runtime.datastruct import KeyValue
from ..runtime.environment.tracing import Tracer, start_stream_span
from ..runtime.store import JoinStorageFactory, JoinStorage
from ..runtime.store.storage import JoinStorageConfig
from .functions import JoinFunction


class _JoinStorageConfig(JoinStorageConfig):
    __slots__ = ('_name', '_ttl', '_renew_ttl')

    def __init__(self, cfg: JoinStreamConfig) -> None:
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


class JoinFunctionContext[K: Hashable, T1, T2, R](StreamFunction[R]):
    _fn: JoinFunction[K, T1, T2, R]

    def __init__(self, stream: TypedStream[R], fn: JoinFunction[K, T1, T2, R]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]) -> bool:
        self.before_call()
        result = await self._fn.join(self._stream, key, left_values, right_values, out)
        self.after_call()
        return result


class JoinStream[K: Hashable, T1, T2, R](TypedJoinConsumedStream[K, T1, T2, R]):
    _source: TypedStream[KeyValue[K, T1]]
    _f: JoinFunctionContext[K, T1, T2, R]
    _join_link: "JoinLink[K, T1, T2, R]"
    _join_storage: JoinStorage[K]
    _collector: Collector[R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: JoinStreamConfig, stream: TypedStream[KeyValue[K, T1]],
                 right_stream: TypedStream[KeyValue[K, T2]],
                 fn: JoinFunction[K, T1, T2, R]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = JoinFunctionContext[K, T1, T2, R](self, fn)
        self._join_type = cfg.join_type
        self._collector = Collector[R](None)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self
        self._join_link = JoinLink[K, T1, T2, R](self, right_stream)
        self._join_storage = JoinStorageFactory[K]().make_storage(
            cfg.join_storage, env, _JoinStorageConfig(cfg))
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
        from ..api.models.join_type import JoinType

        async def _join_callback(values: list[list[Any]]) -> bool:
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
                return await self._f.call(key, values[0], values[1], self._collector)
            return False

        await self._join_storage.join_value(key, index, value, _join_callback)

    async def consume(self, value: KeyValue[K, T1]) -> None:
        _, span = start_stream_span(self._tracer, "stream.join", self)
        try:
            with span.scoped():
                await self._consume(value.key, 0, value.value)
        finally:
            span.end()

    async def consume_right(self, value: KeyValue[K, T2]) -> None:
        await self._consume(value.key, 1, value.value)


class JoinLink[K: Hashable, T1, T2, R](Stream, StreamConsumer[KeyValue[K, T2]]):
    _join_stream: JoinStream[K, T1, T2, R]
    _source: TypedStream[KeyValue[K, T2]]

    def __init__(self, join_stream: JoinStream[K, T1, T2, R], stream: TypedStream[KeyValue[K, T2]]):
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
        return self._join_stream

    async def consume(self, value: KeyValue[K, T2]) -> None:
        await self._join_stream.consume_right(value)

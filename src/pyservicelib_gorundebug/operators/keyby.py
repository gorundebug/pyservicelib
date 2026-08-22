#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Optional

from ..runtime.common import StreamFunction, Collect, Collector, TypedStream, TypedTransformConsumedStream, RuntimeKeyValueHelpers
from ..runtime.config.stream_types import KeyByStreamConfig
from ..runtime.datastruct import KeyValue
from ..runtime.environment.tracing import Tracer, sampling_enabled, start_stream_span
from .functions import KeyByFunction


class KeyByFunctionContext[T, K: Hashable, V](StreamFunction[KeyValue[K, V]]):
    _fn: KeyByFunction[T, K, V]

    def __init__(self, stream: TypedStream[KeyValue[K, V]], fn: KeyByFunction[T, K, V]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, value: T, out: Collect[KeyValue[K, V]]):
        self.before_call()
        await self._fn.key_by(self._stream, value, out)
        self.after_call()


class KeyByStream[T, K, V](TypedTransformConsumedStream[T, KeyValue[K, V]]):
    _source: TypedStream[T]
    _f: KeyByFunctionContext[T, K, V]
    _collector: Collector[KeyValue[K, V]]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: KeyByStreamConfig, stream: TypedStream[T], fn: KeyByFunction[T, K, V]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeKeyValueHelpers[K, V](env).make_key_value_stream_serde(
                             key_type_name=cfg.key_type, value_type_name=cfg.value_type))
        self._source = stream
        self._f = KeyByFunctionContext[T, K, V](self, fn)
        self._collector = Collector[KeyValue[K, V]](None)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    @property
    def consumer(self):
        return self._consumer

    @consumer.setter
    def consumer(self, value):
        self._consumer = value
        self._caller = RuntimeKeyValueHelpers[K, V](self.environment).make_caller(self)
        self._collector = Collector(self._caller)

    async def consume(self, value: T) -> None:
        if self._tracer is None or not sampling_enabled():
            await self._f.call(value, self._collector)
            return
        _, span = start_stream_span(self._tracer, "stream.keyby", self)
        try:
            with span.scoped():
                await self._f.call(value, self._collector)
        finally:
            span.end()

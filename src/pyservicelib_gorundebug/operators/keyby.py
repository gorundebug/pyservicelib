#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable, Optional

from ..runtime.common import StreamFunction, TypedStream, TypedTransformConsumedStream, RuntimeKeyValueHelpers
from ..runtime.config.stream_types import KeyByStreamConfig
from ..runtime.datastruct import KeyValue
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from .functions import KeyByFunction


class KeyByFunctionContext[T, K: Hashable, V](StreamFunction[KeyValue[K, V]]):
    _fn: KeyByFunction[T, K, V]

    def __init__(self, context: TypedStream[KeyValue[K, V]], fn: KeyByFunction[T, K, V]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> KeyValue[K, V]:
        self.before_call()
        result = await self._fn.key_by(self._context, value)
        self.after_call()
        return result


class KeyByStream[T, K, V](TypedTransformConsumedStream[T, KeyValue[K, V]]):
    _source: TypedStream[T]
    _f: KeyByFunctionContext[T, K, V]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: KeyByStreamConfig, stream: TypedStream[T], fn: KeyByFunction[T, K, V]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeKeyValueHelpers[K, V](env).make_key_value_stream_serde(
                             key_type_name=cfg.key_type, value_type_name=cfg.value_type))
        self._source = stream
        self._f = KeyByFunctionContext[T, K, V](self, fn)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.keyby",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                kv = await self._f.call(value)
                if self._caller is not None:
                    await self._caller.consume(kv)
        finally:
            span.end()

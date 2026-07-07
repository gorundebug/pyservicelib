#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import StreamFunction, TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from ..runtime.config.stream_types import MapStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from .functions import MapFunction


class MapFunctionContext[T, R](StreamFunction[R]):
    _fn: MapFunction[T, R]

    def __init__(self, context: TypedStream[R], fn: MapFunction[T, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> R:
        self.before_call()
        result = await self._fn.map(self._context, value)
        self.after_call()
        return result


class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _f: MapFunctionContext[T, R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: MapStreamConfig, stream: TypedStream[T], fn: MapFunction[T, R]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = MapFunctionContext[T, R](self, fn)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.map",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                v = await self._f.call(value)
                if self._caller is not None:
                    await self._caller.consume(v)
        finally:
            span.end()

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import StreamFunction, TypedStream, TypedConsumedStream
from ..runtime.config.stream_types import FilterStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from .functions import FilterFunction


class FilterFunctionContext[T](StreamFunction[T]):
    _fn: FilterFunction[T]

    def __init__(self, context: TypedStream[T], fn: FilterFunction[T]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> bool:
        self.before_call()
        result = await self._fn.filter(self._context, value)
        self.after_call()
        return result


class FilterStream[T](TypedConsumedStream[T]):
    _source: TypedStream[T]
    _f: FilterFunctionContext[T]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: FilterStreamConfig, stream: TypedStream[T], fn: FilterFunction[T]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env, serde=stream.serde)
        self._source = stream
        self._f = FilterFunctionContext[T](self, fn)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.filter",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if await self._f.call(value):
                    if self._caller is not None:
                        await self._caller.consume(value)
        finally:
            span.end()

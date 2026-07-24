#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from datetime import timedelta
from typing import Optional

from ..runtime.common import StreamFunction, TypedStream, TypedConsumedStream
from ..runtime.config.stream_types import DelayStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr
from .functions import DelayFunction


class DelayFunctionContext[T](StreamFunction[T]):
    _fn: DelayFunction[T]

    def __init__(self, stream: TypedStream[T], fn: DelayFunction[T]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, value: T) -> timedelta:
        self.before_call()
        result = await self._fn.duration(self._stream, value)
        self.after_call()
        return result


class DelayStream[T](TypedConsumedStream[T]):
    _source: TypedStream[T]
    _f: DelayFunctionContext[T]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: DelayStreamConfig, stream: TypedStream[T], fn: DelayFunction[T]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env, serde=stream.serde)
        self._source = stream
        self._f = DelayFunctionContext[T](self, fn)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer, "stream.delay", string_attr("stream", self.name))
        duration = await self._f.call(value)
        if duration.total_seconds() > 0.0:
            if self._caller is not None:
                caller = self._caller

                async def _delayed(v: T) -> None:
                    try:
                        with span.scoped():
                            await caller.consume(v)
                    finally:
                        span.end()

                await self.environment.delay(duration, _delayed, value)
            else:
                span.end()
        else:
            try:
                with span.scoped():
                    if self._caller is not None:
                        await self._caller.consume(value)
            finally:
                span.end()

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from datetime import timedelta
from typing import Optional

from ..runtime.common import (
    Collect,
    Collector,
    StreamFunction,
    TypedConsumedStream,
    TypedStream,
)
from ..runtime.config.stream_types import DelayStreamConfig
from ..runtime.context.request import request_context_error
from ..runtime.durable_context import durable_call_delay
from ..runtime.environment.tracing import (
    Tracer,
    sampling_enabled,
    span_error,
    span_event,
    start_stream_span,
    string_attr,
)
from .functions import DelayFunction


class DelayFunctionContext[T](StreamFunction[T]):
    _fn: DelayFunction[T]

    def __init__(self, stream: TypedStream[T], fn: DelayFunction[T]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, value: T) -> timedelta:
        self.before_call()
        try:
            return await self._fn.duration(self._stream, value)
        finally:
            self.after_call()

    async def call_error(
        self,
        value: T,
        error: Exception,
        out: "Collect[T]",
    ) -> None:
        self.before_call()
        try:
            await self._fn.delay_error(
                self._stream,
                value,
                error,
                out,
            )
        finally:
            self.after_call()


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
        if self._tracer is None or not sampling_enabled():
            duration = await self._f.call(value)
            if duration.total_seconds() > 0.0:
                if self._caller is None:
                    return
                caller = self._caller

                async def _delayed_untraced(v: T) -> None:
                    if request_context_error() is None:
                        await caller.consume(v)

                try:
                    if await durable_call_delay(duration):
                        await _delayed_untraced(value)
                    else:
                        await self.environment.delay(
                            duration, _delayed_untraced, value
                        )
                except Exception as error:
                    await self._f.call_error(
                        value,
                        error,
                        Collector(self._caller),
                    )
                return
            if self._caller is not None:
                await self._caller.consume(value)
            return

        _, span = start_stream_span(self._tracer, "stream.delay", self)
        duration = await self._f.call(value)
        if duration.total_seconds() > 0.0:
            if self._caller is not None:
                caller = self._caller

                async def _delayed(v: T) -> None:
                    try:
                        with span.scoped():
                            reason = request_context_error()
                            if reason is not None:
                                span_event(
                                    span,
                                    "delay.skipped",
                                    string_attr("reason", reason),
                                )
                                return
                            await caller.consume(v)
                    finally:
                        span.end()

                try:
                    if await durable_call_delay(duration):
                        await _delayed(value)
                    else:
                        await self.environment.delay(duration, _delayed, value)
                except Exception as error:
                    try:
                        span_error(span, error)
                        await self._f.call_error(
                            value,
                            error,
                            Collector(self._caller),
                        )
                    finally:
                        span.end()
            else:
                span.end()
        else:
            try:
                with span.scoped():
                    if self._caller is not None:
                        await self._caller.consume(value)
            finally:
                span.end()

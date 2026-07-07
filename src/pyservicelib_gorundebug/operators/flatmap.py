#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import StreamFunction, Collect, Collector, TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from ..runtime.config.stream_types import FlatMapStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from .functions import FlatMapFunction


class FlatMapFunctionContext[T, R](StreamFunction[R]):
    _fn: FlatMapFunction[T, R]

    def __init__(self, context: TypedStream[R], fn: FlatMapFunction[T, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T, out: Collect[R]):
        self.before_call()
        await self._fn.flatmap(self._context, value, out)
        self.after_call()


class FlatMapStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _f: FlatMapFunctionContext[T, R]
    _collector: Collector[R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: FlatMapStreamConfig, stream: TypedStream[T], fn: FlatMapFunction[T, R]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = FlatMapFunctionContext[T, R](self, fn)
        self._collector = Collector[R](None)
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    @property
    def consumer(self):
        return self._consumer

    @consumer.setter
    def consumer(self, value):
        self._consumer = value
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)
        self._collector = Collector(self._caller)

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.flatmap",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                await self._f.call(value, self._collector)
        finally:
            span.end()

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import StreamFunction, Collect, Collector, TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from ..runtime.config.stream_types import MapStreamConfig
from ..runtime.environment.tracing import Tracer, sampling_enabled, start_stream_span
from .functions import MapFunction


class MapFunctionContext[T, R](StreamFunction[R]):
    _fn: MapFunction[T, R]

    def __init__(self, stream: TypedStream[R], fn: MapFunction[T, R]):
        super().__init__(stream)
        self._fn = fn

    async def call(self, value: T, out: Collect[R]):
        self.before_call()
        await self._fn.map(self._stream, value, out)
        self.after_call()


class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _f: MapFunctionContext[T, R]
    _collector: Collector[R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: MapStreamConfig, stream: TypedStream[T], fn: MapFunction[T, R]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = MapFunctionContext[T, R](self, fn)
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
        if self._tracer is None or not sampling_enabled():
            await self._f.call(value, self._collector)
            return
        _, span = start_stream_span(self._tracer, "stream.map", self)
        try:
            with span.scoped():
                await self._f.call(value, self._collector)
        finally:
            span.end()

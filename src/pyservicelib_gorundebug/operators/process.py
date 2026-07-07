#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import (
    StreamFunction, Collect, Collector, TypedStream,
    TypedTransformConsumedStream, TypedConsumedStream, RuntimeHelpers, ErrorStream,
)
from ..runtime.config.stream_types import ProcessStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled
from ..runtime.serde.serde import StreamSerde, StubSerde
from .functions import ProcessFunction


class ProcessFunctionContext[T, R, E](StreamFunction[R]):
    _fn: ProcessFunction[T, R, E]

    def __init__(self, context: TypedStream[R], fn: ProcessFunction[T, R, E]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T, out: Collect[R], err_out: Collect[E]):
        self.before_call()
        await self._fn.process(self._context, value, out, err_out)
        self.after_call()


class ProcessStream[T, R, E](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _f: ProcessFunctionContext[T, R, E]
    _error_stream: ErrorStream[E]
    _out_collector: Collector[R]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: ProcessStreamConfig, stream: TypedStream[T], fn: ProcessFunction[T, R, E]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = ProcessFunctionContext[T, R, E](self, fn)
        self._error_stream = ErrorStream[E](
            stream_id=cfg.id,
            env=env,
            serde=StreamSerde(StubSerde('error')),
        )
        self._out_collector = Collector[R](None)
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
        self._out_collector = Collector(self._caller)

    @property
    def error_stream(self) -> ErrorStream[E]:
        return self._error_stream

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.process",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                await self._f.call(value, self._out_collector, self._error_stream)
        finally:
            span.end()

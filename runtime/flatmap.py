#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.common import StreamFunction, TypedStreamSerde, Collect, Collector
from pyservicelib.runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from pyservicelib.runtime.functions import FlatMapFunction


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
    _serdeIn: TypedStreamSerde[T]
    _f: FlatMapFunctionContext[T, R]

    def __init__(self, name: str, stream: TypedStream[T], fn: FlatMapFunction[T, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"FlatMapStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the FlatMapStream with name '{name}' is not defined")

        super().__init__(cfg=cfg,
                         serde=RuntimeHelpers[R](stream.environment).make_serde(type_name=cfg.value_type),
                         env=stream.environment)
        self._source = stream
        self._serdeIn = stream.serde
        self._f = FlatMapFunctionContext[T, R](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        if self._caller is not None:
           await self._f.call(value, Collector(self._caller))



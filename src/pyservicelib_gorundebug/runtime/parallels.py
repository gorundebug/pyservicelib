#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from .common import StreamFunction, Collect, ParallelsCollector
from .common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from .functions import ParallelsFunction


class ParallelsFunctionContext[T, R](StreamFunction[R]):
    _fn: ParallelsFunction[T, R]

    def __init__(self, context: TypedStream[R], fn: ParallelsFunction[T, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T, out: Collect[R]):
        self.before_call()
        await self._fn.parallels(self._context, value, out)
        self.after_call()


class ParallelsStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _f: ParallelsFunctionContext[T, R]

    def __init__(self, name: str, stream: TypedStream[T], fn: ParallelsFunction[T, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"ParallelsStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the ParallelsStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=stream.environment,
                         serde=RuntimeHelpers[R](stream.environment).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        self._f = ParallelsFunctionContext[T, R](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._f.call(value, ParallelsCollector[R](self._caller, self._environment))



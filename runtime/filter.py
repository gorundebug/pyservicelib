#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.environment import StreamFunction
from pyservicelib.runtime.environment import TypedStream, TypedConsumedStream
from pyservicelib.runtime.stream import FilterFunction


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

    def __init__(self, name: str, stream: TypedStream[T], fn: FilterFunction[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"FilterStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the FilterStream with name '{name}' is not defined")

        super().__init__(cfg=cfg,
                         serde=stream.serde,
                         env=stream.environment)
        self._source = stream
        self._f = FilterFunctionContext(self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            if await self._f.call(value):
                await self._caller.consume(value)

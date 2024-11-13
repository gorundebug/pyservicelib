#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from .common import StreamFunction
from .common import TypedStream, TypedConsumedStream, RuntimeHelpers
from .functions import ForEachFunction


class ForEachFunctionContext[T](StreamFunction[T]):
    _fn: ForEachFunction[T]

    def __init__(self, context: TypedStream[T], fn: ForEachFunction[T]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T):
        self.before_call()
        await self._fn.for_each(self._context, value)
        self.after_call()


class ForEachStream[T](TypedConsumedStream[T]):
    _source: TypedStream[T]
    _f: ForEachFunctionContext[T]

    def __init__(self, name: str, stream: TypedStream[T], fn: ForEachFunction[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"ForEachStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the ForEachStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)
        self._source = stream
        self._f = ForEachFunctionContext[T](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        await self._f.call(value)
        if self._caller is not None:
            await self._caller.consume(value)

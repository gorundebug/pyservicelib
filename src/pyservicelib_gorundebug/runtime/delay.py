#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from datetime import timedelta

from .common import StreamFunction
from .common import TypedStream, TypedConsumedStream
from .functions import DelayFunction


class DelayFunctionContext[T](StreamFunction[T]):
    _fn: DelayFunction[T]

    def __init__(self, context: TypedStream[T], fn: DelayFunction[T]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> timedelta:
        self.before_call()
        result = await self._fn.duration(self._context, value)
        self.after_call()
        return result


class DelayStream[T](TypedConsumedStream[T]):
    _source: TypedStream[T]
    _f: DelayFunctionContext[T]

    def __init__(self, name: str, stream: TypedStream[T], fn: DelayFunction[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"DelayStream configuration names '{name}' not found")

        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)
        self._source = stream
        self._f = DelayFunctionContext[T](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        duration = await self._f.call(value)
        if duration.total_seconds() > 0.0:
            if self._caller is not None:
                await self.environment.delay(duration, self._caller.consume, value)
        else:
            if self._caller is not None:
                await self._caller.consume(value)
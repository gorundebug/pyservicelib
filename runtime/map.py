#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.common import StreamFunction, TypedStreamSerde
from pyservicelib.runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from pyservicelib.runtime.functions import MapFunction


class MapFunctionContext[T, R](StreamFunction[R]):
    _fn: MapFunction[T, R]

    def __init__(self, context: TypedStream[R], fn: MapFunction[T, R]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> R:
        self.before_call()
        result = await self._fn.map(self._context, value)
        self.after_call()
        return result


class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _in_serde: TypedStreamSerde[T]
    _f: MapFunctionContext[T, R]

    def __init__(self, name: str, stream: TypedStream[T], fn: MapFunction[T, R]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"MapStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the MapStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id,
                         serde=RuntimeHelpers[R](stream.environment).make_serde(type_name=cfg.value_type),
                         env=stream.environment)
        self._source = stream
        self._in_serde = stream.serde
        self._f = MapFunctionContext[T, R](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        v = await self._f.call(value)
        if self._caller is not None:
            await self._caller.consume(v)

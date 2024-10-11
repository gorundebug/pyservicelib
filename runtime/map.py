#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.environment import StreamFunction, TypedStreamSerde
from pyservicelib.runtime.environment import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from pyservicelib.runtime.stream import MapFunction


class MapFunctionContext[T, R](StreamFunction[R]):
    _fn: MapFunction[T, R]

    def __init__(self, context: TypedStream[R], fn: MapFunction[T, R]):
        super().__init__(context)
        self._fn = fn

    def call(self, value: T) -> R:
        self.before_call()
        result = self._fn.map(self._context, value)
        self.after_call()
        return result


class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _serdeIn: TypedStreamSerde[T]
    _f: MapFunctionContext[T, R]

    def __init__(self, name: str, stream: TypedStream[T], fn: MapFunction[T, R]):
        super().__init__(name, RuntimeHelpers[R](stream.environment).make_serde(), stream.environment)
        self._source = stream
        self._serdeIn = stream.serde
        self._f = MapFunctionContext(self, fn)
        stream.consumer = self

    def consume(self, value: T) -> None:
        if self._caller is not None:
            self._caller.consume(self._f.call(value))

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.environment import TypedStream, TypedTransformConsumedStream
from pyservicelib.runtime.runtime import RuntimeTypeHelpers

class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    def __init__(self, name: str, stream: TypedStream[T]):
        super().__init__(name, RuntimeTypeHelpers[R](stream.environment.runtime).make_serde(), stream.environment)

    def consume(self, value: T) -> None:
        if self._caller is not None:
            self._caller.consume(value)

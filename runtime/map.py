#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.environment import TypedStream, TypedTransformConsumedStream, RuntimeTypeHelpers

class MapStream[T, R](TypedTransformConsumedStream[T, R]):
    def __init__(self, name: str, stream: TypedStream[T]):
        super().__init__(name, RuntimeTypeHelpers[R](stream.environment).make_serde(), stream.environment)
        stream.consumer = self

    def consume(self, value: T) -> None:
        pass

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from stream import ConsumedStream
from environment import StreamExecutionRuntime

class InputStream[T](ConsumedStream[T]):

    def __init__(self, name: str, runtime: StreamExecutionRuntime):
        super().__init__(name, runtime)



#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from environment import Stream

class MapFunction[T, R](ABC):

    @abstractmethod
    def map(self, context: Stream, value: T) -> R:
        pass
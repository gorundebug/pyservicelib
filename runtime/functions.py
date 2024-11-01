#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from abc import ABC, abstractmethod
from common import Stream, Collect


class MapFunction[T, R](ABC):

    @abstractmethod
    async def map(self, context: Stream, value: T) -> R:
        pass


class FilterFunction[T](ABC):

    @abstractmethod
    async def filter(self, context: Stream, value: T) -> bool:
        pass


class DelayFunction[T](ABC):

    @abstractmethod
    async def duration(self, context: Stream, value: T) -> timedelta:
        pass


class FlatMapFunction[T, R](ABC):

    @abstractmethod
    async def flatmap(self, context: Stream, value: T, out: Collect[R]) -> R:
        pass
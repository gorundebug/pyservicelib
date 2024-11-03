#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from abc import ABC, abstractmethod
from typing import Hashable

from pyservicelib.runtime.common import ServiceStream, Collect


class MapFunction[T, R](ABC):

    @abstractmethod
    async def map(self, context: ServiceStream, value: T) -> R:
        pass


class FilterFunction[T](ABC):

    @abstractmethod
    async def filter(self, context: ServiceStream, value: T) -> bool:
        pass


class DelayFunction[T](ABC):

    @abstractmethod
    async def duration(self, context: ServiceStream, value: T) -> timedelta:
        pass


class FlatMapFunction[T, R](ABC):

    @abstractmethod
    async def flatmap(self, context: ServiceStream, value: T, out: Collect[R]):
        pass


class JoinFunction[K: Hashable, T1, T2, R](ABC):

    @abstractmethod
    async def join(self, context: ServiceStream, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]):
        pass
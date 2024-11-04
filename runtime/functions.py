#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from abc import ABC, abstractmethod
from typing import Hashable

from pyservicelib.runtime.common import Stream, Collect
from pyservicelib.runtime.datastruct import KeyValue


class MapFunction[T, R](ABC):

    @abstractmethod
    async def map(self, context: Stream, value: T) -> R:
        pass


class ParallelsFunction[T, R](ABC):

    @abstractmethod
    async def parallels(self, context: Stream, value: T, out: Collect[R]):
        pass


class KeyByFunction[T, K: Hashable, V](ABC):

    @abstractmethod
    async def key_by(self, context: Stream, value: T) -> KeyValue[K, V]:
        pass


class FilterFunction[T](ABC):

    @abstractmethod
    async def filter(self, context: Stream, value: T) -> bool:
        pass


class ForEachFunction[T](ABC):

    @abstractmethod
    async def for_each(self, context: Stream, value: T):
        pass


class DelayFunction[T](ABC):

    @abstractmethod
    async def duration(self, context: Stream, value: T) -> timedelta:
        pass


class FlatMapFunction[T, R](ABC):

    @abstractmethod
    async def flatmap(self, context: Stream, value: T, out: Collect[R]):
        pass


class JoinFunction[K: Hashable, T1, T2, R](ABC):

    @abstractmethod
    async def join(self, context: Stream, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]):
        pass
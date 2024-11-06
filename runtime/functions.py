#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from abc import ABC, abstractmethod
from typing import Hashable, Any, Protocol

from pyservicelib.runtime.common import Stream, Collect
from pyservicelib.runtime.datastruct import KeyValue


class MapFunction[T, R](Protocol):

    @abstractmethod
    async def map(self, context: Stream, value: T) -> R:
        ...


class ParallelsFunction[T, R](Protocol):

    @abstractmethod
    async def parallels(self, context: Stream, value: T, out: Collect[R]):
        ...


class KeyByFunction[T, K: Hashable, V](Protocol):

    @abstractmethod
    async def key_by(self, context: Stream, value: T) -> KeyValue[K, V]:
        ...


class FilterFunction[T](Protocol):

    @abstractmethod
    async def filter(self, context: Stream, value: T) -> bool:
        ...


class ForEachFunction[T](Protocol):

    @abstractmethod
    async def for_each(self, context: Stream, value: T):
        ...


class DelayFunction[T](Protocol):

    @abstractmethod
    async def duration(self, context: Stream, value: T) -> timedelta:
        ...


class FlatMapFunction[T, R](Protocol):

    @abstractmethod
    async def flatmap(self, context: Stream, value: T, out: Collect[R]):
        ...


class JoinFunction[K: Hashable, T1, T2, R](Protocol):

    @abstractmethod
    async def join(self, context: Stream, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]):
        ...


class MultiJoinFunction[K: Hashable, T, R](Protocol):

    @abstractmethod
    async def multi_join(self, context: Stream, key: K, values: list[list[Any]], out: Collect[R]):
        ...
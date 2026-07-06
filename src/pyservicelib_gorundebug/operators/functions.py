#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from typing import Hashable, Any, Protocol

from ..runtime.common import Stream, Collect
from ..runtime.datastruct import KeyValue


class MapFunction[T, R](Protocol):
    async def map(self, context: Stream, value: T) -> R: ...


class FilterFunction[T](Protocol):
    async def filter(self, context: Stream, value: T) -> bool: ...


class FlatMapFunction[T, R](Protocol):
    async def flatmap(self, context: Stream, value: T, out: Collect[R]): ...


class JoinFunction[K: Hashable, T1, T2, R](Protocol):
    async def join(self, context: Stream, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]): ...


class MultiJoinFunction[K: Hashable, T, R](Protocol):
    async def multi_join(self, context: Stream, key: K, values: list[list[Any]], out: Collect[R]): ...


class KeyByFunction[T, K: Hashable, V](Protocol):
    async def key_by(self, context: Stream, value: T) -> KeyValue[K, V]: ...


class ProcessFunction[T, R, E](Protocol):
    async def process(self, context: Stream, value: T, out: Collect[R], err_out: Collect[E]): ...


class DelayFunction[T](Protocol):
    async def duration(self, context: Stream, value: T) -> timedelta: ...
    async def delay_error(self, context: Stream, value: T, error: Exception, out: Collect[T]): ...

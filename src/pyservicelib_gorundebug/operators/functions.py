#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from typing import Hashable, Any, Protocol, Callable

from ..runtime.common import Stream, Collect
from ..runtime.datastruct import KeyValue


class MapFunction[T, R](Protocol):
    async def map(self, context: Stream, value: T, out: Collect[R]): ...


class FilterFunction[T](Protocol):
    async def filter(self, context: Stream, value: T) -> bool: ...


class FlatMapFunction[T, R](Protocol):
    async def flatmap(self, context: Stream, value: T, out: Collect[R]): ...


class JoinFunction[K: Hashable, T1, T2, R](Protocol):
    async def join(self, context: Stream, key: K, left_values: list[T1], right_values: list[T2], out: Collect[R]) -> bool: ...


class MultiJoinFunction[K: Hashable, T, R](Protocol):
    async def multi_join(self, context: Stream, key: K, values: list[list[Any]], out: Collect[R]) -> bool: ...


class KeyByFunction[T, K: Hashable, V](Protocol):
    async def key_by(self, context: Stream, value: T, out: Collect[KeyValue[K, V]]): ...


class ProcessFunction[T, R, E](Protocol):
    async def process(self, context: Stream, value: T, out: Collect[R], err_out: Collect[E]): ...


class DelayFunction[T](Protocol):
    async def duration(self, context: Stream, value: T) -> timedelta: ...
    async def delay_error(self, context: Stream, value: T, error: Exception, out: Collect[T]): ...


class When(Protocol):
    @property
    def type(self) -> type: ...
    def get_when_consumer(self) -> Stream: ...


class BuildSwitchFunction[T](Protocol):
    def build_switch(self, stream: Stream, when_items: list[When]) -> Callable[[T], int]: ...


class BuildSwitchHandler[T]:
    _fn: Callable[[Stream, list[When]], Callable[[T], int]]

    def __init__(self, fn: Callable[[Stream, list[When]], Callable[[T], int]]):
        self._fn = fn

    def build_switch(self, stream: Stream, when_items: list[When]) -> Callable[[T], int]:
        return self._fn(stream, when_items)


def default_build_switch[T](when_items: list[When]) -> Callable[[T], int]:
    type_map: dict[type, int] = {}
    for i, w in enumerate(when_items):
        type_map[w.type] = i

    def switch_fn(value: T) -> int:
        t = type(value)
        if t in type_map:
            return type_map[t]
        raise ValueError(f"Unknown type: {t!r} in case switch")

    return switch_fn

from typing import TypeVar

from pyservicelib_gorundebug.runtime.common import CollectFunc

T = TypeVar("T")


def collector(values: list[T]) -> CollectFunc[T]:
    async def append(value: T) -> None:
        values.append(value)

    return CollectFunc(append)

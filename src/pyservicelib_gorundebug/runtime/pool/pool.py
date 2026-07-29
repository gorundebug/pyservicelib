#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from contextvars import Context as ContextVarsContext
from typing import Callable, Any, Optional
from datetime import timedelta, datetime
import asyncio

from ..context import Context


class _AsyncWaitGroup:
    """asyncio equivalent of sync.WaitGroup."""
    __slots__ = ('_count', '_event')

    def __init__(self) -> None:
        self._count = 0
        self._event = asyncio.Event()
        self._event.set()

    def add(self) -> None:
        if self._count == 0:
            self._event.clear()
        self._count += 1

    def done(self) -> None:
        self._count -= 1
        if self._count == 0:
            self._event.set()

    async def wait(self) -> None:
        await self._event.wait()


class PoolAlreadyStartedError(RuntimeError):
    def __init__(self):
        super().__init__("pool already started")


class PoolStoppedError(RuntimeError):
    def __init__(self):
        super().__init__("pool stopped")


class PoolNotStartedError(RuntimeError):
    def __init__(self):
        super().__init__("pool not started")


class PoolCancelledError(RuntimeError):
    def __init__(self):
        super().__init__("request cancelled")


@dataclass(slots=True)
class _PoolTask:
    fn: Callable[..., Any]
    args: tuple
    kwargs: dict
    deadline: Optional[datetime]            # wall-clock, for restoring request_deadline ContextVar
    deadline_ts: Optional[float]            # monotonic (loop.time()), for context watcher
    cancelled_event: Optional[asyncio.Event] = field(default=None, compare=False)  # for restoring request_cancelled ContextVar
    context: Optional[ContextVarsContext] = field(default=None, compare=False)
    state: str = "delayed"                  # "delayed" | "running"
    after_task: Optional[asyncio.Task] = field(default=None, compare=False)
    expedited: bool = field(default=False, compare=False)


class Pool(ABC):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass


class DelayPool(Pool):

    @abstractmethod
    async def add_task(self, delay: timedelta, fn: Callable[..., Any], *args, **kwargs) -> None:
        pass


class PriorityTaskPool(Pool):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def add_task(self, priority: int, fn: Callable[..., Any], *args, **kwargs) -> None:
        pass


class TaskPool(Pool):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def add_task(self, fn: Callable[..., Any], *args, **kwargs) -> None:
        pass

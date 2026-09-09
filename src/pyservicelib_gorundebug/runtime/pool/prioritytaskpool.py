#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Awaitable, Callable
from ..common import ServiceEnvironment
from .pool import PriorityTaskPool
from ._queuedpool import QueuedPool


class PriorityTaskPoolImpl(QueuedPool, PriorityTaskPool):
    def __init__(self, name: str, env: ServiceEnvironment):
        super().__init__(name, env, priority=True)

    async def add_task(self, priority: int, fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> None:
        await self._add(priority, fn, args, kwargs)


def make_priority_task_pool(name: str, env: ServiceEnvironment) -> PriorityTaskPool:
    return PriorityTaskPoolImpl(name, env)

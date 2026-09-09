#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Awaitable, Callable
from ..common import ServiceEnvironment
from .pool import TaskPool
from ._queuedpool import QueuedPool


class TaskPoolImpl(QueuedPool, TaskPool):
    def __init__(self, name: str, env: ServiceEnvironment):
        super().__init__(name, env, priority=False)

    async def add_task(self, fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> None:
        await self._add(0, fn, args, kwargs)


def make_task_pool(name: str, env: ServiceEnvironment) -> TaskPool:
    return TaskPoolImpl(name, env)

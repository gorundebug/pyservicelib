#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Callable, Any, List
import asyncio

from pyservicelib.runtime.common import ServiceEnvironment
from pyservicelib.runtime.pool import TaskPool
from pyservicelib.runtime.context import Context


class TaskPoolImpl(TaskPool):
    _environment: ServiceEnvironment
    _task_queue: asyncio.Queue
    _executors: List[asyncio.Task[Any]]
    _name: str

    def __init__(self, name: str, env: ServiceEnvironment):
        cfg = env.config.get_pool_by_name(name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{name}' not found")
        self._environment = env
        self._task_queue = asyncio.Queue()
        self._executors = []
        self._name = name

    async def _executor(self):
        while True:
            task, args, kwargs = await self._task_queue.get()
            if task is None:
                break
            try:
                await task(*args, **kwargs)
            finally:
                self._task_queue.task_done()

    async def start(self, ctx: Context):
        cfg = self._environment.config.get_pool_by_name(self._name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{self._name}' not found")
        executors_count = cfg.executors_count or 1
        for _ in range(executors_count):
            executor_task = asyncio.create_task(self._executor())
            self._executors.append(executor_task)

    async def stop(self, ctx: Context):
        for _ in self._executors:
            await self._task_queue.put((None, None, None))

        try:
            await asyncio.wait_for(asyncio.gather(*self._executors), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            tasks_count = self._task_queue.qsize()
            self._environment.log.warning(f"Task pool '{self._name}' stopped by timeout. (tasks count={tasks_count})")

    async def add_task(self, fn: Callable[..., Any], *args, **kwargs):
        await self._task_queue.put((fn, args, kwargs))


def make_task_pool(name: str, env: ServiceEnvironment) -> TaskPool:
    return TaskPoolImpl(name, env)
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import abstractmethod
from typing import Callable, Any, List
import asyncio

from pyservicelib.runtime.common import ServiceEnvironment
from pyservicelib.runtime.pool import Pool
from pyservicelib.runtime.context import Context

class TaskPool(Pool):

    @abstractmethod
    async def add_task(self, fn: Callable[..., Any], *args, **kwargs):
        pass


class TaskPoolImpl:
    _environment: ServiceEnvironment
    _task_queue: asyncio.Queue
    _executors: List[asyncio.Task[Any]]
    _executors_count: int
    _name: str

    def __init__(self, name: str, env: ServiceEnvironment):
        cfg = env.config.get_pool_by_name(name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{name}' not found")
        self._environment = env
        self._task_queue = asyncio.Queue()
        self._executors = []
        self._name = name

    async def executor(self):
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
        executors_count = cfg.executors_count
        for _ in range(executors_count):
            executor_task = asyncio.create_task(self.executor())
            self._executors.append(executor_task)

    async def stop(self, ctx: Context):
        for _ in self._executors:
            await self._task_queue.put((None, None, None))

        try:
            await asyncio.wait_for(asyncio.gather(*self._executors), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            tasks_count = self._task_queue.qsize()
            #print(f"Task pool '{self._name}' не смог завершиться: превышен дедлайн "
            #      f"(осталось задач: {tasks_count})")

    async def add_task(self, task: Callable[..., Any], *args, **kwargs):
        await self._task_queue.put((task, args, kwargs))


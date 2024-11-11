#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Callable, Any, Optional
import asyncio
from datetime import datetime, timedelta

from pyservicelib.runtime.common import ServiceEnvironment
from pyservicelib.runtime.pool import DelayPool
from pyservicelib.runtime.context import Context


class DelayPoolImpl(DelayPool):
    _environment: ServiceEnvironment
    _task_queue: asyncio.Queue
    _priority_task_queue: asyncio.PriorityQueue
    _executors: list[asyncio.Task[Any]]
    _timer_executor_task: Optional[asyncio.Task[Any]]
    _new_task_event: asyncio.Event
    _stopping: bool

    def __init__(self, env: ServiceEnvironment):
        self._environment = env
        self._priority_task_queue = asyncio.PriorityQueue()
        self._task_queue = asyncio.Queue()
        self._executors = []
        self._new_task_event = asyncio.Event()
        self._stopping = False
        self._timer_executor_task = None

    async def _executor(self):
        while True:
            task, args, kwargs = await self._task_queue.get()
            if task is None:
                break
            try:
                await task(*args, **kwargs)
            finally:
                self._task_queue.task_done()

    async def _timer_executor(self):
        while not self._stopping or not self._priority_task_queue.empty():

            if not self._priority_task_queue.empty():
                execute_time, task, args, kwargs = await self._priority_task_queue.get()
                delay: float = (execute_time - datetime.now()).total_seconds()
                if delay > 0.0:
                    try:
                        await asyncio.wait_for(self._new_task_event.wait(), timeout=delay)
                        self._new_task_event.clear()
                        await self._priority_task_queue.put((execute_time, task, args, kwargs))
                        continue
                    except asyncio.TimeoutError:
                        pass

                self._priority_task_queue.task_done()
                await self._task_queue.put((task, args, kwargs))
            else:
                await self._new_task_event.wait()
                self._new_task_event.clear()

        for _ in self._executors:
            await self._task_queue.put((None, None, None))

    async def start(self, ctx: Context):
        executors_count = self._environment.service_config.delay_executors or 1
        for _ in range(executors_count):
            executor_task = asyncio.create_task(self._executor())
            self._executors.append(executor_task)

        self._timer_executor_task = asyncio.create_task(self._timer_executor())

    async def stop(self, ctx: Context):
        if self._timer_executor_task is not None:
            self._stopping = True
            self._new_task_event.set()

            executors = self._executors + [self._timer_executor_task]
            try:
                await asyncio.wait_for(asyncio.gather(*executors), timeout=ctx.time_left)
            except asyncio.TimeoutError:
                tasks_count = self._priority_task_queue.qsize()
                self._environment.log.warning(
                    f"Delay pool stopped by timeout. (tasks count={tasks_count})"
                )

    async def add_task(self, delay: timedelta, fn: Callable[..., Any], *args, **kwargs):
        execute_time = datetime.now() + delay
        await self._priority_task_queue.put((execute_time, fn, args, kwargs))
        self._new_task_event.set()


def make_delay_pool(env: ServiceEnvironment) -> DelayPool:
    return DelayPoolImpl(env)

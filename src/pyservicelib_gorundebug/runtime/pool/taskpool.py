#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import time
from typing import Callable, Awaitable, Any, Optional
import asyncio
from datetime import datetime, timezone

from ..common import ServiceEnvironment
from .pool import TaskPool, PoolAlreadyStartedError, PoolNotStartedError, PoolStoppedError, PoolCancelledError, _PoolTask, _AsyncWaitGroup
from ..context import Context
from ..context.request import request_deadline, request_cancelled
from ..environment.metrics import Int64Counter, Int64Gauge, Float64Histogram
from ..environment.log import str_field, int_field, err_field


class TaskPoolImpl(TaskPool):
    _environment: ServiceEnvironment
    _task_queue: asyncio.Queue[_PoolTask | None]
    _executors: list[asyncio.Task[Any]]
    _all_executors: set[asyncio.Task[Any]]
    _executor_manager_task: Optional[asyncio.Task[Any]]
    _name: str
    _started: bool
    _stopped: bool
    _lock: asyncio.Lock
    _after_tasks: set[asyncio.Task]
    _running_tasks: set[asyncio.Task]   # detached create_task instances (immediate deadline + _after_func)
    _wg: _AsyncWaitGroup

    _gauge_queue_length: Int64Gauge
    _tasks_total: Int64Counter
    _execution_duration: Float64Histogram
    _stop_timeout_counter: Int64Counter
    _task_rejected_counter: Int64Counter
    _task_cancelled_counter: Int64Counter

    def __init__(self, name: str, env: ServiceEnvironment):
        cfg = env.config.get_pool_by_name(name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{name}' not found")
        self._environment = env
        self._task_queue = asyncio.Queue()
        self._executors = []
        self._all_executors = set()
        self._executor_manager_task = None
        self._name = name
        self._started = False
        self._stopped = False
        self._lock = asyncio.Lock()
        self._after_tasks = set()
        self._running_tasks = set()
        self._wg = _AsyncWaitGroup()

        scope = env.metrics.scope('task_pool', {'service': env.service_config.name, 'name': name})
        self._gauge_queue_length = scope.gauge('queue_length', 'Task pool wait queue length', {})
        self._tasks_total = scope.counter('tasks_total', 'Total number of tasks executed by task pool', {})
        self._execution_duration = scope.histogram('task_execution_duration_seconds', 'Task execution duration in seconds', {})
        self._stop_timeout_counter = scope.counter('events_total', 'Total number of events in task pool', {'event': 'stop_timeout'})
        self._task_rejected_counter = scope.counter('events_total', 'Total number of events in task pool', {'event': 'task_rejected'})
        self._task_cancelled_counter = scope.counter('events_total', 'Total number of events in task pool', {'event': 'task_cancelled'})

    @property
    def name(self) -> str:
        return self._name

    def _spawn_executor(self) -> asyncio.Task[Any]:
        t = asyncio.create_task(self._executor())
        self._all_executors.add(t)
        t.add_done_callback(self._all_executors.discard)
        return t

    async def _run_task(self, task: _PoolTask) -> None:
        token = request_deadline.set(task.deadline) if task.deadline is not None else None
        cancelled_token = request_cancelled.set(task.cancelled_event) if task.cancelled_event is not None else None
        try:
            # Always dispatch — cancelled/expired tasks still reach user code for cleanup/release.
            if task.cancelled_event is not None and task.cancelled_event.is_set():
                self._task_cancelled_counter.inc()
            start = time.monotonic()
            await task.fn(*task.args, **task.kwargs)
            self._tasks_total.inc()
            self._execution_duration.observe(time.monotonic() - start)
        except Exception as e:
            self._environment.log.warn('task pool task error',
                                       str_field('pool', self._name), err_field(e))
        finally:
            if token is not None:
                request_deadline.reset(token)
            if cancelled_token is not None:
                request_cancelled.reset(cancelled_token)
            self._wg.done()

    async def _after_func(self, task: _PoolTask) -> None:
        assert task.deadline_ts is not None
        try:
            await asyncio.sleep(max(0.0, task.deadline_ts - asyncio.get_running_loop().time()))
        except asyncio.CancelledError:
            return
        async with self._lock:
            if task.state != 'delayed':
                return
            task.state = 'running'
        # create_task so stop() cancelling _after_tasks does not interrupt the running fn
        t = asyncio.create_task(self._run_task(task))
        self._running_tasks.add(t)
        t.add_done_callback(self._running_tasks.discard)

    async def _executor(self):
        while True:
            task = await self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            # Decrement here — task has left the queue regardless of whether we run it
            self._gauge_queue_length.dec()
            if task.after_task is not None:
                task.after_task.cancel()
            run = False
            async with self._lock:
                if task.state == 'delayed':
                    task.state = 'running'
                    run = True
            if run:
                await self._run_task(task)
            # else: _after_func already claimed the task and called _run_task; wg.done() is its responsibility
            self._task_queue.task_done()

    async def _executor_manager(self, initial_count: int) -> None:
        executors_count = initial_count
        try:
            while True:
                await asyncio.sleep(1.0)
                async with self._lock:
                    if self._stopped:
                        return
                cfg = self._environment.config.get_pool_by_name(self._name)
                if cfg is None:
                    continue
                new_count = cfg.executors_count or 1
                if new_count == executors_count:
                    continue

                old_executors = self._executors[:]
                old_count = len(old_executors)

                # Clear current list before sending sentinels — no new executors exist
                # yet, so sentinels can only be consumed by old executors (no race).
                self._executors = []

                if old_executors:
                    # Track completion via done-callbacks, not gather().
                    # gather() would cancel old executor tasks if manager is cancelled;
                    # callbacks fire independently regardless of manager lifetime.
                    remaining = len(old_executors)
                    old_done = asyncio.Event()

                    def _on_done(_t: asyncio.Task) -> None:
                        nonlocal remaining
                        remaining -= 1
                        if remaining == 0:
                            old_done.set()

                    for t in old_executors:
                        t.add_done_callback(_on_done)

                    for _ in old_executors:
                        await self._task_queue.put(None)

                    # Wait for all old executors to stop before spawning new ones.
                    # If manager is cancelled here, old executors keep draining on their
                    # own; stop() will handle them via _all_executors.
                    await old_done.wait()

                self._executors = [self._spawn_executor() for _ in range(new_count)]
                executors_count = new_count

                self._environment.log.info(
                    'task pool executor count changed',
                    str_field('pool', self._name),
                    int_field('old_count', old_count),
                    int_field('new_count', new_count),
                )
        except asyncio.CancelledError:
            pass

    async def start(self, ctx: Context):
        async with self._lock:
            if self._stopped:
                raise PoolStoppedError()
            if self._started:
                raise PoolAlreadyStartedError()
            self._started = True
        cfg = self._environment.config.get_pool_by_name(self._name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{self._name}' not found")
        executors_count = cfg.executors_count or 1
        self._executors = [self._spawn_executor() for _ in range(executors_count)]
        self._executor_manager_task = asyncio.create_task(self._executor_manager(executors_count))

    async def stop(self, ctx: Context):
        async with self._lock:
            if self._stopped:
                return
            self._stopped = True

        # Stop manager first so it cannot spawn new executors or modify _all_executors
        if self._executor_manager_task is not None:
            self._executor_manager_task.cancel()
            await asyncio.gather(self._executor_manager_task, return_exceptions=True)

        after_tasks = list(self._after_tasks)
        for t in after_tasks:
            t.cancel()
        await asyncio.gather(*after_tasks, return_exceptions=True)
        self._after_tasks.clear()

        # Snapshot all live executors (current + any draining from a previous reload)
        all_executors = list(self._all_executors)
        if not all_executors:
            await self._wg.wait()
            return

        for _ in all_executors:
            await self._task_queue.put(None)

        async def _wait_all():
            await self._wg.wait()
            await asyncio.gather(*all_executors, return_exceptions=True)

        wait_task = asyncio.ensure_future(_wait_all())
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=ctx.time_left)
            return
        except asyncio.TimeoutError:
            pass

        self._environment.log.warn('task pool stopped by timeout',
                                   str_field('pool', self._name),
                                   int_field('tasks_count', self._task_queue.qsize()))
        self._stop_timeout_counter.inc()
        await wait_task

    async def add_task(self, fn: Callable[..., Awaitable[Any]], *args, **kwargs):
        async with self._lock:
            if self._stopped:
                self._task_rejected_counter.inc()
                raise PoolStoppedError()
            if not self._started:
                self._task_rejected_counter.inc()
                raise PoolNotStartedError()

        deadline = request_deadline.get()
        cancelled_event = request_cancelled.get()

        if cancelled_event is not None and cancelled_event.is_set():
            self._task_rejected_counter.inc()
            raise PoolCancelledError()

        loop = asyncio.get_running_loop()
        deadline_ts: Optional[float] = None
        if deadline is not None:
            if deadline.tzinfo is None:
                now = datetime.now()
                remaining = (deadline - now).total_seconds()
            else:
                now = datetime.now(timezone.utc)
                remaining = (deadline.astimezone(timezone.utc) - now).total_seconds()
            deadline_ts = loop.time() + max(0.0, remaining)
        task = _PoolTask(fn=fn, args=args, kwargs=kwargs, deadline=deadline, deadline_ts=deadline_ts,
                         cancelled_event=cancelled_event)

        self._wg.add()

        if deadline_ts is not None and deadline_ts <= loop.time():
            # Already expired — run directly, never touches the queue
            t = asyncio.create_task(self._run_task(task))
            self._running_tasks.add(t)
            t.add_done_callback(self._running_tasks.discard)
            return

        # Only tasks that enter the queue contribute to queue_length
        self._gauge_queue_length.inc()

        if deadline_ts is not None:
            after_task = asyncio.create_task(self._after_func(task))
            task.after_task = after_task
            self._after_tasks.add(after_task)
            after_task.add_done_callback(self._after_tasks.discard)

        try:
            await self._task_queue.put(task)
        except BaseException:
            if task.after_task is not None:
                task.after_task.cancel()
                self._after_tasks.discard(task.after_task)
            self._wg.done()
            self._gauge_queue_length.dec()
            raise


def make_task_pool(name: str, env: ServiceEnvironment) -> TaskPool:
    return TaskPoolImpl(name, env)

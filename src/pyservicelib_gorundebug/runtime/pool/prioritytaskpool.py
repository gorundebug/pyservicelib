#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import time
from contextvars import copy_context
from typing import Callable, Awaitable, Any, Optional
import asyncio
from datetime import datetime, timezone

from ..common import ServiceEnvironment
from .pool import (
    PriorityTaskPool,
    PoolAlreadyStartedError,
    PoolNotStartedError,
    PoolStoppedError,
    PoolCancelledError,
    _PoolTask,
    _AsyncWaitGroup,
)
from ..context import Context
from ..context.request import (
    request_cancelled,
    request_context_error,
    request_deadline,
)
from ..environment.metrics import Int64Counter, Int64Gauge, Float64Histogram
from ..environment.log import str_field, int_field, err_field


class PriorityTaskPoolImpl(PriorityTaskPool):
    _environment: ServiceEnvironment
    _task_queue: asyncio.PriorityQueue[tuple[int | float, int, _PoolTask | None]]
    _executors: list[asyncio.Task[Any]]
    _all_executors: set[asyncio.Task[Any]]
    _executor_manager_task: Optional[asyncio.Task[Any]]
    _name: str
    _started: bool
    _stopped: bool
    _counter: int
    _lock: asyncio.Lock
    _after_tasks: set[asyncio.Task]
    _running_tasks: set[asyncio.Task]
    _wg: _AsyncWaitGroup

    _gauge_queue_length: Int64Gauge
    _gauge_executors_target: Int64Gauge
    _gauge_executors_allocated: Int64Gauge
    _gauge_executors_busy: Int64Gauge
    _tasks_total: Int64Counter
    _execution_duration: Float64Histogram
    _stop_timeout_counter: Int64Counter
    _task_rejected_counter: Int64Counter
    _task_expired_counter: Int64Counter

    def __init__(self, name: str, env: ServiceEnvironment):
        cfg = env.config.get_pool_by_name(name)
        if cfg is None:
            raise ValueError(
                f"Priority task pool configuration named '{name}' not found"
            )
        self._environment = env
        self._task_queue = asyncio.PriorityQueue()
        self._executors = []
        self._all_executors = set()
        self._executor_manager_task = None
        self._name = name
        self._started = False
        self._stopped = False
        self._counter = 0
        self._lock = asyncio.Lock()
        self._after_tasks = set()
        self._running_tasks = set()
        self._wg = _AsyncWaitGroup()

        scope = env.metrics.scope(
            "priority_task_pool", {"service": env.service_config.name, "name": name}
        )
        self._gauge_queue_length = scope.gauge(
            "queue_length", "Priority task pool wait queue length", {}
        )
        self._gauge_executors_target = scope.gauge(
            "executors_target", "Desired number of priority task pool executors", {}
        )
        self._gauge_executors_allocated = scope.gauge(
            "executors_allocated", "Number of live priority task pool executors", {}
        )
        self._gauge_executors_busy = scope.gauge(
            "executors_busy",
            "Number of priority task pool executors running callbacks",
            {},
        )
        self._tasks_total = scope.counter(
            "tasks_total", "Total number of tasks executed by priority task pool", {}
        )
        self._execution_duration = scope.histogram(
            "task_execution_duration_seconds", "Task execution duration in seconds", {}
        )
        self._stop_timeout_counter = scope.counter(
            "events_total",
            "Total number of events in priority task pool",
            {"event": "stop_timeout"},
        )
        self._task_rejected_counter = scope.counter(
            "events_total",
            "Total number of events in priority task pool",
            {"event": "task_rejected"},
        )
        self._task_expired_counter = scope.counter(
            "events_total",
            "Total number of events in priority task pool",
            {"event": "task_expired"},
        )

    @property
    def name(self) -> str:
        return self._name

    def _spawn_executor(self) -> asyncio.Task[Any]:
        self._gauge_executors_allocated.inc()
        try:
            t = asyncio.create_task(self._executor())
        except BaseException:
            self._gauge_executors_allocated.dec()
            raise
        self._all_executors.add(t)

        def _on_done(done: asyncio.Task[Any]) -> None:
            self._all_executors.discard(done)
            self._gauge_executors_allocated.dec()

        t.add_done_callback(_on_done)
        return t

    async def _run_task(self, task: _PoolTask) -> None:
        token = (
            request_deadline.set(task.deadline) if task.deadline is not None else None
        )
        cancelled_token = (
            request_cancelled.set(task.cancelled_event)
            if task.cancelled_event is not None
            else None
        )
        try:
            # Always dispatch — cancelled/expired tasks still reach user code for cleanup/release.
            start = time.monotonic()
            await task.fn(*task.args, **task.kwargs)
            self._tasks_total.inc()
            self._execution_duration.observe(time.monotonic() - start)
        except Exception as e:
            self._environment.log.warn(
                "priority task pool task error",
                str_field("pool", self._name),
                err_field(e),
            )
        finally:
            if token is not None:
                request_deadline.reset(token)
            if cancelled_token is not None:
                request_cancelled.reset(cancelled_token)
            self._wg.done()

    async def _watch_context(self, task: _PoolTask) -> None:
        try:
            if task.cancelled_event is None:
                assert task.deadline_ts is not None
                await asyncio.sleep(
                    max(
                        0.0,
                        task.deadline_ts - asyncio.get_running_loop().time(),
                    )
                )
            elif task.deadline_ts is None:
                await task.cancelled_event.wait()
            else:
                try:
                    async with asyncio.timeout_at(task.deadline_ts):
                        await task.cancelled_event.wait()
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            return
        async with self._lock:
            if task.state != "delayed":
                return
            seq = self._counter
            self._counter += 1
            self._task_queue.put_nowait((float("-inf"), seq, task))
        self._task_expired_counter.inc()

    async def _run_in_request_context(self, task: _PoolTask) -> None:
        runner = asyncio.create_task(
            self._run_task(task),
            context=task.context.copy() if task.context is not None else None,
        )
        self._running_tasks.add(runner)
        runner.add_done_callback(self._running_tasks.discard)
        await runner

    async def _executor(self):
        while True:
            _priority, _seq, task = await self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            if task.after_task is not None:
                task.after_task.cancel()
            run = False
            async with self._lock:
                if task.state == "delayed":
                    task.state = "running"
                    run = True
            if run:
                self._gauge_queue_length.dec()
                self._gauge_executors_busy.inc()
                try:
                    await self._run_in_request_context(task)
                finally:
                    self._gauge_executors_busy.dec()
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

                self._gauge_executors_target.set(new_count)

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
                        seq = self._counter
                        self._counter += 1
                        # float('-inf') = highest priority: old executor exits after its
                        # current task, before picking up any new real task (mirrors Go's
                        # *pRestart check before cond.Wait / heap.Pop).
                        # Negative sequence also places the sentinel before an
                        # already-promoted real task at the same -inf priority.
                        await self._task_queue.put((float("-inf"), -seq - 1, None))

                    # Wait for all old executors to stop before spawning new ones.
                    # If manager is cancelled here, old executors keep draining on their
                    # own; stop() will handle them via _all_executors.
                    await old_done.wait()

                self._executors = [self._spawn_executor() for _ in range(new_count)]
                executors_count = new_count

                self._environment.log.info(
                    "priority task pool executor count changed",
                    str_field("pool", self._name),
                    int_field("old_count", old_count),
                    int_field("new_count", new_count),
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
            raise ValueError(
                f"Priority task pool configuration named '{self._name}' not found"
            )
        executors_count = cfg.executors_count or 1
        self._gauge_executors_target.set(executors_count)
        self._executors = [self._spawn_executor() for _ in range(executors_count)]
        self._executor_manager_task = asyncio.create_task(
            self._executor_manager(executors_count)
        )

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
            seq = self._counter
            self._counter += 1
            await self._task_queue.put((float("inf"), seq, None))

        async def _wait_all():
            await self._wg.wait()
            await asyncio.gather(*all_executors, return_exceptions=True)

        wait_task = asyncio.ensure_future(_wait_all())
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=ctx.time_left)
            return
        except asyncio.TimeoutError:
            pass

        self._environment.log.warn(
            "priority task pool stopped by timeout",
            str_field("pool", self._name),
            int_field("tasks_count", self._task_queue.qsize()),
        )
        self._stop_timeout_counter.inc()
        await wait_task

    async def add_task(
        self, priority: int, fn: Callable[..., Awaitable[Any]], *args, **kwargs
    ):
        if request_context_error() is not None:
            self._task_rejected_counter.inc()
            raise PoolCancelledError()

        deadline = request_deadline.get()
        cancelled_event = request_cancelled.get()
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
        task = _PoolTask(
            fn=fn,
            args=args,
            kwargs=kwargs,
            deadline=deadline,
            deadline_ts=deadline_ts,
            cancelled_event=cancelled_event,
            context=copy_context(),
        )

        async with self._lock:
            if self._stopped:
                self._task_rejected_counter.inc()
                raise PoolStoppedError()
            if not self._started:
                self._task_rejected_counter.inc()
                raise PoolNotStartedError()

            self._wg.add()
            self._gauge_queue_length.inc()
            seq = self._counter
            self._counter += 1
            self._task_queue.put_nowait((priority, seq, task))
            if deadline_ts is not None or cancelled_event is not None:
                after_task = asyncio.create_task(self._watch_context(task))
                task.after_task = after_task
                self._after_tasks.add(after_task)
                after_task.add_done_callback(self._after_tasks.discard)


def make_priority_task_pool(name: str, env: ServiceEnvironment) -> PriorityTaskPool:
    return PriorityTaskPoolImpl(name, env)

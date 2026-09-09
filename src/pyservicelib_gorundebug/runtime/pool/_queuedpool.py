#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from contextvars import Context as VariablesContext
import os
import time
from collections import OrderedDict
from ..common import ServiceEnvironment
from ..context import Context
from .pool import PoolAlreadyStartedError, PoolStoppedError, PoolCancelledError, _AsyncWaitGroup
from ._scheduling import IndexedHeap, ContextWatches, make_task, await_drain
from ..context.request import request_context_error
from ..environment.log import str_field, int_field, err_field


class QueuedPool:
    def __init__(self, name: str, env: ServiceEnvironment, *, priority):
        cfg = env.config.get_pool_by_name(name)
        if cfg is None:
            raise ValueError(f"Task pool configuration named '{name}' not found")
        self._environment = env
        self._name = name
        self._priority = priority
        self._fallback_count = cfg.executors_count
        self._queue = IndexedHeap() if priority else OrderedDict()
        self._watches = ContextWatches()
        self._changed = asyncio.Event()
        self._executors = {}
        self._all_executors = set()
        self._running_tasks = set()
        self._executor_manager_task = None
        self._stop_task = None
        self._started = False
        self._stopped = False
        self._counter = 0
        self._target = self._configured_count()
        self._wg = _AsyncWaitGroup()
        scope = env.metrics.scope(
            "priority_task_pool" if priority else "task_pool", {"service": env.service_config.name, "name": name}
        )
        self._gauge_queue_length = scope.gauge(
            "queue_length", "Task pool wait queue length", {}
        )
        self._gauge_executors_target = scope.gauge(
            "executors_target", "Desired number of task pool executors", {}
        )
        self._gauge_executors_allocated = scope.gauge(
            "executors_allocated", "Number of live task pool executors", {}
        )
        self._gauge_executors_busy = scope.gauge(
            "executors_busy", "Number of task pool executors running callbacks", {}
        )
        self._tasks_total = scope.counter(
            "tasks_total", "Total number of tasks executed by task pool", {}
        )
        self._execution_duration = scope.histogram(
            "task_execution_duration_seconds", "Task execution duration in seconds", {}
        )
        self._stop_timeout_counter = scope.counter(
            "events_total",
            "Total number of events in task pool",
            {"event": "stop_timeout"},
        )
        self._task_rejected_counter = scope.counter(
            "events_total",
            "Total number of events in task pool",
            {"event": "task_rejected"},
        )
        self._task_cancelled_counter = scope.counter(
            "events_total",
            "Total number of events in task pool",
            {"event": "task_expired" if priority else "task_cancelled"},
        )

    @property
    def name(self) -> str:
        return self._name

    def _configured_count(self):
        cfg = self._environment.config.get_pool_by_name(self._name)
        count = cfg.executors_count if cfg is not None else self._fallback_count
        if count < 0:
            raise ValueError("executors_count must be non-negative")
        return count or os.cpu_count() or 1

    def _promote(self, task):
        if task.state != "delayed":
            return
        if self._priority:
            moved = self._queue.promote(task)
        else:
            moved = bool(self._queue) and next(iter(self._queue)) != id(task)
            if moved:
                self._queue.move_to_end(id(task), last=False)
        if moved:
            self._task_cancelled_counter.inc()
        self._changed.set()

    async def _add(self, priority, fn, args, kwargs):
        if request_context_error() is not None:
            self._task_rejected_counter.inc()
            raise PoolCancelledError()
        if self._stopped:
            self._task_rejected_counter.inc()
            raise PoolStoppedError()
        task = make_task(fn, args, kwargs)
        if self._priority:
            self._queue.push(task, (priority, self._counter))
        else:
            self._queue[id(task)] = task
        self._counter += 1
        self._wg.add()
        self._gauge_queue_length.inc()
        self._watches.add(task, self._promote)
        self._changed.set()

    def _ensure_executors(self):
        self._gauge_executors_target.set(self._target)
        for slot in range(self._target):
            current = self._executors.get(slot)
            if current is not None and not current.done():
                continue
            worker = asyncio.create_task(self._executor(slot), context=VariablesContext())
            self._executors[slot] = worker
            self._all_executors.add(worker)
            self._gauge_executors_allocated.inc()
            def done(task, slot=slot):
                self._all_executors.discard(task)
                self._gauge_executors_allocated.dec()
                if self._executors.get(slot) is task:
                    self._executors.pop(slot)
                if not self._stopped and slot < self._target:
                    self._ensure_executors()
            worker.add_done_callback(done)
        self._changed.set()

    async def _run_task(self, task):
        start = time.monotonic()
        try:
            await task.fn(*task.args, **task.kwargs)
        except (Exception, asyncio.CancelledError) as error:
            try:
                self._environment.log.warn("task pool task error", str_field("pool", self._name), err_field(error))
            except Exception:
                pass
        finally:
            self._tasks_total.inc()
            self._execution_duration.observe(time.monotonic() - start)
            self._wg.done()

    async def _executor(self, slot):
        while slot < self._target:
            # Retiring workers still count towards concurrency until their
            # current callback finishes after a configuration downsize.
            if not self._queue or len(self._running_tasks) >= self._target:
                if self._stopped and not self._queue:
                    return
                self._changed.clear()
                await self._changed.wait()
                continue
            if self._priority:
                _, task = self._queue.pop()
            else:
                _, task = self._queue.popitem(last=False)
            task.state = "running"
            self._watches.remove(task)
            self._gauge_queue_length.dec()
            self._gauge_executors_busy.inc()
            # Each request keeps its complete ContextVars state across awaits.
            runner = asyncio.create_task(self._run_task(task), context=task.context)
            self._running_tasks.add(runner)
            try:
                await runner
            finally:
                self._running_tasks.discard(runner)
                self._gauge_executors_busy.dec()
                self._changed.set()
            del task, runner

    async def _manager(self, ctx):
        while not ctx.is_expired:
            await asyncio.sleep(min(1.0, ctx.time_left) if ctx.time_left is not None else 1.0)
            if self._stopped or ctx.is_expired:
                return
            target = self._configured_count()
            if target != self._target:
                self._target = target
                self._ensure_executors()

    async def start(self, ctx: Context) -> None:
        if self._stopped:
            raise PoolStoppedError()
        if self._started:
            raise PoolAlreadyStartedError()
        self._started = True
        self._target = self._configured_count()
        self._ensure_executors()
        self._executor_manager_task = asyncio.create_task(self._manager(ctx), context=VariablesContext())

    async def _shutdown(self):
        if self._executor_manager_task is not None:
            self._executor_manager_task.cancel()
            await asyncio.gather(self._executor_manager_task, return_exceptions=True)
        self._ensure_executors()
        await self._wg.wait()
        self._changed.set()
        await asyncio.gather(*tuple(self._all_executors), return_exceptions=True)
        await self._watches.close()

    async def stop(self, ctx: Context) -> None:
        if self._stop_task is None:
            self._stopped = True
            self._stop_task = asyncio.create_task(self._shutdown(), context=VariablesContext())
        def report():
            self._environment.log.warn("task pool stopped by timeout", str_field("pool", self._name), int_field("tasks_count", len(self._queue)))
            self._stop_timeout_counter.inc()
        await await_drain(self._stop_task, ctx, report)

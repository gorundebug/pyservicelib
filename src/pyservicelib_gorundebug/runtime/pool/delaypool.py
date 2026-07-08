#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import time
from typing import Callable, Awaitable, Any, Optional
import asyncio
from datetime import datetime, timedelta, timezone

from ..common import ServiceEnvironment
from .pool import DelayPool, PoolAlreadyStartedError, PoolNotStartedError, PoolStoppedError, PoolCancelledError, _PoolTask, _AsyncWaitGroup
from ..context import Context
from ..context.request import request_deadline, request_cancelled
from ..environment.metrics import Int64Counter, Int64Gauge, Float64Histogram
from ..environment.log import str_field, err_field


class DelayPoolImpl(DelayPool):
    _environment: ServiceEnvironment
    _priority_task_queue: asyncio.PriorityQueue[tuple[float, int, _PoolTask]]
    _timer_executor_task: Optional[asyncio.Task[Any]]
    _new_task_event: asyncio.Event
    _started: bool
    _stopped: bool
    _counter: int
    _lock: asyncio.Lock
    _wg: _AsyncWaitGroup
    _running_tasks: set[asyncio.Task]

    _gauge_wait_queue_length: Int64Gauge
    _tasks_total: Int64Counter
    _execution_duration: Float64Histogram
    _stop_timeout_counter: Int64Counter
    _task_cancelled_counter: Int64Counter

    def __init__(self, env: ServiceEnvironment):
        self._environment = env
        self._priority_task_queue = asyncio.PriorityQueue()
        self._new_task_event = asyncio.Event()
        self._started = False
        self._stopped = False
        self._timer_executor_task = None
        self._counter = 0
        self._lock = asyncio.Lock()
        self._wg = _AsyncWaitGroup()
        self._running_tasks = set()

        scope = env.metrics.scope('delay_pool', {'service': env.service_config.name})
        self._gauge_wait_queue_length = scope.gauge('wait_queue_length', 'Delay pool wait queue length', {})
        self._tasks_total = scope.counter('tasks_total', 'Total number of tasks executed by delay pool', {})
        self._execution_duration = scope.histogram('task_execution_duration_seconds', 'Task execution duration in seconds', {})
        self._stop_timeout_counter = scope.counter('events_total', 'Total number of events in delay pool', {'event': 'stop_timeout'})
        self._task_cancelled_counter = scope.counter('events_total', 'Total number of events in delay pool', {'event': 'task_cancelled'})

    async def _run_task(self, task: _PoolTask) -> None:
        token = request_deadline.set(task.deadline) if task.deadline is not None else None
        cancelled_token = request_cancelled.set(task.cancelled_event) if task.cancelled_event is not None else None
        try:
            if task.cancelled_event is not None and task.cancelled_event.is_set():
                self._task_cancelled_counter.inc()
            start = time.monotonic()
            await task.fn(*task.args, **task.kwargs)
            self._tasks_total.inc()
            self._execution_duration.observe(time.monotonic() - start)
        except Exception as e:
            self._environment.log.warn('delay pool task error', err_field(e))
        finally:
            if token is not None:
                request_deadline.reset(token)
            if cancelled_token is not None:
                request_cancelled.reset(cancelled_token)
            self._wg.done()

    def _spawn(self, task: _PoolTask) -> None:
        t = asyncio.create_task(self._run_task(task))
        self._running_tasks.add(t)
        t.add_done_callback(self._running_tasks.discard)

    async def _timer_executor(self):
        loop = asyncio.get_running_loop()
        while not self._stopped or not self._priority_task_queue.empty():
            if not self._priority_task_queue.empty():
                execute_ts, _seq, task = await self._priority_task_queue.get()
                self._gauge_wait_queue_length.dec()

                delay = max(0.0, execute_ts - loop.time())
                if delay > 0.0:
                    try:
                        await asyncio.wait_for(self._new_task_event.wait(), timeout=delay)
                        self._new_task_event.clear()
                        self._gauge_wait_queue_length.inc()
                        await self._priority_task_queue.put((execute_ts, _seq, task))
                        continue
                    except asyncio.TimeoutError:
                        pass

                async with self._lock:
                    task.state = 'running'
                self._priority_task_queue.task_done()
                self._spawn(task)
            else:
                await self._new_task_event.wait()
                self._new_task_event.clear()

    async def start(self, ctx: Context):
        async with self._lock:
            if self._stopped:
                raise PoolStoppedError()
            if self._started:
                raise PoolAlreadyStartedError()
            self._started = True
        self._timer_executor_task = asyncio.create_task(self._timer_executor())

    async def stop(self, ctx: Context):
        async with self._lock:
            if self._stopped:
                return
            self._stopped = True

        if self._timer_executor_task is None:
            await self._wg.wait()
            return

        self._new_task_event.set()

        try:
            await asyncio.wait_for(asyncio.shield(self._timer_executor_task), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            self._timer_executor_task.cancel()
            await asyncio.gather(self._timer_executor_task, return_exceptions=True)

            while not self._priority_task_queue.empty():
                try:
                    _, _, task = self._priority_task_queue.get_nowait()
                    self._priority_task_queue.task_done()
                except Exception:
                    break
                async with self._lock:
                    if task.state != 'delayed':
                        continue
                    task.state = 'running'
                self._spawn(task)

            self._environment.log.warn('delay pool stopped by timeout')
            self._stop_timeout_counter.inc()

        await self._wg.wait()

    async def add_task(self, delay: timedelta, fn: Callable[..., Awaitable[Any]], *args, **kwargs):
        async with self._lock:
            if self._stopped:
                raise PoolStoppedError()
            if not self._started:
                raise PoolNotStartedError()

        loop = asyncio.get_running_loop()
        req_deadline = request_deadline.get()
        cancelled_event = request_cancelled.get()

        if cancelled_event is not None and cancelled_event.is_set():
            raise PoolCancelledError()

        req_deadline_ts: Optional[float] = None
        if req_deadline is not None:
            if req_deadline.tzinfo is None:
                now = datetime.now()
                remaining = (req_deadline - now).total_seconds()
            else:
                now = datetime.now(timezone.utc)
                remaining = (req_deadline.astimezone(timezone.utc) - now).total_seconds()
            req_deadline_ts = loop.time() + max(0.0, remaining)

        execute_ts = loop.time() + delay.total_seconds()
        if req_deadline_ts is not None:
            execute_ts = min(execute_ts, req_deadline_ts)

        task = _PoolTask(fn=fn, args=args, kwargs=kwargs, deadline=req_deadline,
                         deadline_ts=req_deadline_ts, cancelled_event=cancelled_event)

        self._wg.add()

        if req_deadline_ts is not None and req_deadline_ts <= loop.time():
            self._spawn(task)
            return

        self._gauge_wait_queue_length.inc()

        seq = self._counter
        self._counter += 1
        try:
            await self._priority_task_queue.put((execute_ts, seq, task))
        except BaseException:
            self._wg.done()
            self._gauge_wait_queue_length.dec()
            raise
        self._new_task_event.set()


def make_delay_pool(env: ServiceEnvironment) -> DelayPool:
    return DelayPoolImpl(env)

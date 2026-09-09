#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from contextvars import Context as VariablesContext
import time
from datetime import timedelta
from typing import Any, Awaitable, Callable
from ..common import ServiceEnvironment
from ..context import Context
from .pool import DelayPool, PoolAlreadyStartedError, PoolStoppedError, PoolCancelledError, _AsyncWaitGroup
from ._scheduling import IndexedHeap, ContextWatches, make_task, await_drain
from ..context.request import request_context_error
from ..environment.log import err_field


class DelayPoolImpl(DelayPool):
    def __init__(self, env: ServiceEnvironment):
        self._environment = env
        self._queue = IndexedHeap()
        self._watches = ContextWatches()
        self._running_tasks = set()
        self._timer = None
        self._armed_at = None
        self._counter = 0
        self._started = False
        self._stopped = False
        self._stop_task = None
        self._wg = _AsyncWaitGroup()
        scope = env.metrics.scope('delay_pool', {'service': env.service_config.name})
        self._gauge_wait_queue_length = scope.gauge('wait_queue_length', 'Delay pool wait queue length', {})
        self._tasks_total = scope.counter('tasks_total', 'Total number of tasks executed by delay pool', {})
        self._execution_duration = scope.histogram('task_execution_duration_seconds', 'Task execution duration in seconds', {})
        self._stop_timeout_counter = scope.counter('events_total', 'Total number of events in delay pool', {'event': 'stop_timeout'})
        self._task_cancelled_counter = scope.counter('events_total', 'Total number of events in delay pool', {'event': 'task_cancelled'})

    async def start(self, ctx: Context) -> None:
        if self._stopped:
            raise PoolStoppedError()
        if self._started:
            raise PoolAlreadyStartedError()
        self._started = True

    async def add_task(self, delay: timedelta, fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> None:
        if request_context_error() is not None:
            raise PoolCancelledError()
        if self._stopped:
            raise PoolStoppedError()
        task = make_task(fn, args, kwargs)
        when = asyncio.get_running_loop().time() + max(0.0, delay.total_seconds())
        task.expedited = task.deadline_ts is not None and task.deadline_ts < when
        if task.deadline_ts is not None:
            when = min(when, task.deadline_ts)
        self._wg.add()
        self._gauge_wait_queue_length.inc()
        self._queue.push(task, (when, self._counter))
        self._counter += 1
        self._watches.add(task, self._cancel, deadline=False)
        self._arm()

    def _cancel(self, task):
        task.expedited = True
        self._dispatch(task)
        self._arm()

    def _arm(self):
        entry = self._queue.peek()
        when = entry[0][0] if entry else None
        if when == self._armed_at:
            return
        if self._timer is not None:
            self._timer.cancel()
        self._armed_at = when
        self._timer = None if when is None else asyncio.get_running_loop().call_at(when, self._ready, context=VariablesContext())

    def _ready(self):
        self._armed_at = None
        self._timer = None
        now = asyncio.get_running_loop().time()
        while self._queue.peek() is not None and self._queue.peek()[0][0] <= now:
            _, task = self._queue.pop()
            self._dispatch(task)
        self._arm()

    def _dispatch(self, task):
        if task.state != "delayed":
            return
        task.state = "running"
        self._queue.remove(task)
        self._watches.remove(task)
        runner = asyncio.create_task(self._run_task(task), context=task.context)
        self._running_tasks.add(runner)
        runner.add_done_callback(self._running_tasks.discard)

    async def _run_task(self, task):
        start = time.monotonic()
        try:
            await task.fn(*task.args, **task.kwargs)
        except (Exception, asyncio.CancelledError) as error:
            try:
                self._environment.log.warn("delay pool task error", err_field(error))
            except Exception:
                pass
        finally:
            if task.expedited:
                self._task_cancelled_counter.inc()
            self._tasks_total.inc()
            self._execution_duration.observe(time.monotonic() - start)
            self._gauge_wait_queue_length.dec()
            self._wg.done()

    async def _shutdown(self):
        await self._wg.wait()
        await asyncio.gather(*tuple(self._running_tasks), return_exceptions=True)
        await self._watches.close()

    async def stop(self, ctx: Context) -> None:
        if self._stop_task is None:
            self._stopped = True
            self._stop_task = asyncio.create_task(self._shutdown(), context=VariablesContext())
        def report():
            self._environment.log.warn("delay pool stopped by timeout")
            self._stop_timeout_counter.inc()
        await await_drain(self._stop_task, ctx, report)


def make_delay_pool(env: ServiceEnvironment) -> DelayPool:
    return DelayPoolImpl(env)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

"""Event-loop owned queue primitives. No blocking locks or stale heap entries."""
import asyncio
from contextvars import Context as VariablesContext, copy_context
from datetime import datetime, timezone
from .pool import _PoolTask
from ..context.request import request_deadline, request_cancelled


class IndexedHeap:
    def __init__(self):
        self.items = []
        self.positions = {}

    def __len__(self):
        return len(self.items)

    def peek(self):
        return self.items[0] if self.items else None

    def push(self, task, key):
        self.positions[id(task)] = len(self.items)
        self.items.append((key, task))
        self._up(len(self.items) - 1)

    def pop(self):
        key, task = self.items[0]
        self.remove(task)
        return key, task

    def remove(self, task):
        index = self.positions.pop(id(task), None)
        if index is None:
            return False
        last = self.items.pop()
        if index < len(self.items):
            self.items[index] = last
            self.positions[id(last[1])] = index
            self._down(self._up(index))
        return True

    def promote(self, task):
        index = self.positions.get(id(task))
        if index is None:
            return False
        key, value = self.items[index]
        self.items[index] = ((float('-inf'), key[1]), value)
        self._up(index)
        return True

    def _swap(self, a, b):
        self.items[a], self.items[b] = self.items[b], self.items[a]
        self.positions[id(self.items[a][1])] = a
        self.positions[id(self.items[b][1])] = b

    def _up(self, index):
        while index:
            parent = (index - 1) // 2
            if self.items[parent][0] <= self.items[index][0]:
                break
            self._swap(index, parent)
            index = parent
        return index

    def _down(self, index):
        while 2 * index + 1 < len(self.items):
            child = 2 * index + 1
            if child + 1 < len(self.items) and self.items[child + 1][0] < self.items[child][0]:
                child += 1
            if self.items[index][0] <= self.items[child][0]:
                break
            self._swap(index, child)
            index = child


def make_task(fn, args, kwargs):
    deadline = request_deadline.get()
    deadline_ts = None
    if deadline is not None:
        now = datetime.now() if deadline.tzinfo is None else datetime.now(timezone.utc)
        deadline_ts = asyncio.get_running_loop().time() + max(0.0, (deadline - now).total_seconds())
    return _PoolTask(fn=fn, args=args, kwargs=kwargs, deadline=deadline,
                     deadline_ts=deadline_ts, cancelled_event=request_cancelled.get(), context=copy_context())


class ContextWatches:
    """One waiter per request Event and one timer for all queued deadlines."""
    def __init__(self):
        self.groups = {}
        self.callbacks = {}
        self.deadlines = IndexedHeap()
        self.timer = None
        self.armed_at = None
        self.sequence = 0
        self.waiters = set()

    def add(self, task, callback, *, deadline=True):
        event = task.cancelled_event
        if event is None and (not deadline or task.deadline_ts is None):
            return
        self.callbacks[id(task)] = (task, callback)
        if event is not None:
            group = self.groups.get(event)
            if group is None:
                tasks = set()
                waiter = asyncio.create_task(self._wait(event, tasks), context=VariablesContext())
                self.waiters.add(waiter)
                waiter.add_done_callback(self.waiters.discard)
                group = (tasks, waiter)
                self.groups[event] = group
            group[0].add(id(task))
        if deadline and task.deadline_ts is not None:
            self.deadlines.push(task, (task.deadline_ts, self.sequence))
            self.sequence += 1
            self._arm()

    async def _wait(self, event, tasks):
        await event.wait()
        self.groups.pop(event, None)
        for task_id in tuple(tasks):
            self._notify(task_id)

    def _notify(self, task_id):
        entry = self.callbacks.get(task_id)
        if entry is not None:
            task, callback = entry
            self.remove(task)
            callback(task)

    def remove(self, task):
        self.callbacks.pop(id(task), None)
        group = self.groups.get(task.cancelled_event)
        if group is not None:
            group[0].discard(id(task))
            if not group[0]:
                self.groups.pop(task.cancelled_event, None)
                group[1].cancel()
        self.deadlines.remove(task)
        self._arm()

    def _arm(self):
        entry = self.deadlines.peek()
        when = entry[0][0] if entry else None
        if when == self.armed_at:
            return
        if self.timer is not None:
            self.timer.cancel()
        self.armed_at = when
        self.timer = None if when is None else asyncio.get_running_loop().call_at(when, self._expire, context=VariablesContext())

    def _expire(self):
        self.armed_at = None
        self.timer = None
        now = asyncio.get_running_loop().time()
        while self.deadlines.peek() is not None and self.deadlines.peek()[0][0] <= now:
            _, task = self.deadlines.pop()
            self._notify(id(task))
        self._arm()

    async def close(self):
        if self.timer is not None:
            self.timer.cancel()
        waiters = tuple(self.waiters)
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


async def await_drain(drain, ctx, on_timeout):
    # asyncio.wait never cancels the shared drain when a stop caller is cancelled.
    async def expiry():
        while not ctx.is_expired:
            remaining = ctx.time_left
            await asyncio.sleep(0.05 if remaining is None else min(0.05, remaining))
    timer = asyncio.create_task(expiry())
    try:
        done, _ = await asyncio.wait((drain, timer), return_when=asyncio.FIRST_COMPLETED)
        if drain not in done:
            on_timeout()
        await asyncio.shield(drain)
    finally:
        timer.cancel()
        await asyncio.gather(timer, return_exceptions=True)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Any, Callable, Optional, Hashable, Awaitable
from datetime import datetime

from ..common import ServiceEnvironment
from .._background import terminate_background_failure
from ..context import Context, request_cancelled, request_deadline
from ..environment.metrics import Int64Gauge, Int64Counter
from ..pool.pool import PoolCancelledError
from ..utils.asyncrwlock import AsyncRWLock
from .storage import JoinStorageConfig, JoinStorage, StoreAlreadyStartedError, StoreStoppedError

# Rotation fires only when live entry count has dropped below 1/SHRINK_FACTOR of the peak,
# avoiding pointless rotations under steady or growing load (mirrors rotatingmap.go).
_SHRINK_FACTOR = 4


class Item[V]:
    deadline: float   # monotonic (loop.time()); float('inf') = no expiry (mirrors Go zero time.Time)
    values: list[list[V]]
    processed: bool
    lock: asyncio.Lock
    after_task: Optional[asyncio.Task[Any]]    # fires at deadline — mirrors context.AfterFunc
    callback: Optional[Callable[[list[list[V]]], Awaitable[bool]]]  # stored for after_task

    def __init__(self, deadline: float, num_streams: int):
        self.values = [[] for _ in range(num_streams)]
        self.deadline = deadline
        self.processed = False
        self.lock = asyncio.Lock()
        self.after_task = None
        self.callback = None
        # The creation context owns expiration even when later messages renew
        # the group's logical TTL. Cancellation without a deadline is not a
        # context.AfterFunc path in the Go implementation.
        self.context_deadline = deadline if request_deadline.get() is not None else None
        self.cancelled = request_cancelled.get() if self.context_deadline is not None else None


class HashMapJoinStorage[K: Hashable, V](JoinStorage[K]):

    _config: JoinStorageConfig
    _environment: ServiceEnvironment
    _current: dict[K, Item[V]]
    _prev: dict[K, Item[V]]
    _high_water_mark: int
    _timer_task: Optional[asyncio.Task[Any]]
    _after_tasks: set[asyncio.Task[Any]]
    _started: bool
    _stopped: bool

    _gauge_count: Int64Gauge
    _evictions_total: Int64Counter
    _metrics_enabled: bool

    def __init__(self, env: ServiceEnvironment, cfg: JoinStorageConfig):
        self._environment = env
        self._config = cfg
        self._current = {}
        self._prev = {}
        self._high_water_mark = 0
        self._timer_task = None
        self._after_tasks = set()
        self._started = False
        self._stopped = False
        self._rotation_lock = AsyncRWLock()
        self._metrics_enabled = env.metrics.enabled

        scope = env.metrics.scope('hashmap_join_storage', {
            'service': env.service_config.name,
            'name': cfg.name,
        })
        self._gauge_count = scope.gauge('count', 'Elements count stored in a join storage', {})
        self._evictions_total = scope.counter('evictions_total',
                                              'Total number of items evicted from join storage by TTL', {})

    async def _rotate(self):
        try:
            while True:
                await asyncio.sleep(self._config.ttl.total_seconds())
                async with self._rotation_lock.write_lock():
                    if self._stopped:
                        return
                    total = len(self._current) + len(self._prev)
                    should_rotate = (
                        self._high_water_mark == 0
                        or total * _SHRINK_FACTOR < self._high_water_mark
                    )
                    if total > self._high_water_mark:
                        self._high_water_mark = total
                    if should_rotate:
                        self._high_water_mark = total
                        new_current: dict[K, Item[V]] = {}
                        rescued = 0
                        for k, v in self._prev.items():
                            if k not in self._current:
                                self._current[k] = v
                                rescued += 1
                        evicted = len(self._prev) - rescued
                        self._prev = self._current
                        self._current = new_current
                        if self._metrics_enabled and evicted > 0:
                            self._gauge_count.sub(evicted)
                            self._evictions_total.add(evicted)
        except asyncio.CancelledError:
            pass

    async def _after_func(self, key: K, item: Item[V]) -> None:
        """Fires at request deadline: calls callback with accumulated values, then removes item.
        Mirrors context.AfterFunc in Go pools — ensures items don't linger after context expiry.
        """
        loop = asyncio.get_running_loop()
        while True:
            expiration = item.context_deadline if item.context_deadline is not None else item.deadline
            remaining = max(0.0, expiration - loop.time())
            try:
                if item.cancelled is None:
                    await asyncio.sleep(remaining)
                else:
                    try:
                        async with asyncio.timeout(remaining):
                            await item.cancelled.wait()
                    except TimeoutError:
                        pass
            except asyncio.CancelledError:
                return
            async with item.lock:
                if item.processed:
                    return
                # A timer may already be queued when a callback renews TTL.
                # Recheck under the item lock; context deadlines stay absolute.
                if item.context_deadline is None and item.deadline > loop.time():
                    continue
                item.processed = True
                break
        try:
            # Callback ownership and task ContextVars remain those of the
            # first message, not those of a later renewal.
            if item.callback is not None:
                await item.callback(item.values)
        finally:
            async with self._rotation_lock.read_lock():
                removed = self._remove_item(key, item)
                if removed and self._metrics_enabled:
                    self._evictions_total.inc()

    def _remove_item(self, key: K, item: Item[V]) -> bool:
        # An expired callback may finish after a new generation was admitted.
        if self._current.get(key) is item:
            del self._current[key]
        elif self._prev.get(key) is item:
            del self._prev[key]
        else:
            return False
        if self._metrics_enabled:
            self._gauge_count.dec()
        return True

    def _make_after_task(self, key: K, item: Item[V]) -> None:
        after_task = asyncio.create_task(self._after_func(key, item))
        item.after_task = after_task
        self._after_tasks.add(after_task)
        after_task.add_done_callback(self._after_task_complete)

    def _after_task_complete(self, task: asyncio.Task[Any]) -> None:
        self._after_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and not isinstance(error, PoolCancelledError):
            terminate_background_failure(error)

    async def join_value(self, key: K, index: int, value: V, callback: Callable[[list[list[V]]], Awaitable[bool]]):
        # Mirrors Go's: if ctxDeadline, ok := ctx.Deadline(); ok { ttl = time.Until(ctxDeadline) }
        req_deadline = request_deadline.get()
        if req_deadline is not None:
            now = datetime.now(tz=req_deadline.tzinfo)
            remaining = (req_deadline - now).total_seconds()
            ttl_seconds = max(0.0, remaining)
        else:
            ttl_seconds = self._config.ttl.total_seconds()

        if ttl_seconds > 0:
            async with self._rotation_lock.read_lock():
                await self._join_value(key, index, value, callback, ttl_seconds)
        else:
            await self._join_value(key, index, value, callback, ttl_seconds)

    async def _join_value(self, key: K, index: int, value: V,
                          callback: Callable[[list[list[V]]], Awaitable[bool]],
                          ttl_seconds: float) -> None:
        loop = asyncio.get_running_loop()
        while True:
            # All dict operations below have no await, so they're atomic in asyncio.
            now_ts = loop.time()

            item: Optional[Item[V]] = self._current.get(key)
            if item is None or item.deadline <= now_ts:
                item_prev = self._prev.get(key)
                if item_prev is not None and item_prev.deadline > now_ts:
                    self._current[key] = item_prev
                    del self._prev[key]
                    item = item_prev
                else:
                    if item_prev is not None:
                        del self._prev[key]
                        if self._metrics_enabled:
                            self._gauge_count.dec()
                    item = None

            if item is None:
                # ttl_seconds == 0 → float('inf') mirrors Go's zero time.Time (no individual expiry)
                deadline = (loop.time() + ttl_seconds) if ttl_seconds > 0.0 else float('inf')
                item = Item[V](deadline, index + 1)
                item.callback = callback
                replacing = key in self._current
                self._current[key] = item
                if self._metrics_enabled and not replacing:
                    self._gauge_count.inc()
                # Register after_func for finite deadlines — mirrors context.AfterFunc in Go
                if deadline != float('inf'):
                    self._make_after_task(key, item)

            assert item is not None
            async with item.lock:
                now_ts = loop.time()
                if not item.processed and item.deadline > now_ts:
                    if len(item.values) <= index:
                        item.values.extend([] for _ in range(index - len(item.values) + 1))
                    item.values[index].append(value)
                    item.processed = await callback(item.values)
                    if item.processed:
                        # Cancel after_func — item handled, no need to fire at deadline (stopFn)
                        if item.after_task is not None:
                            item.after_task.cancel()
                        self._remove_item(key, item)
                    elif self._config.renew_ttl and ttl_seconds > 0.0:
                        if (self._current.get(key) is not item
                                and self._prev.get(key) is not item):
                            break
                        item.deadline = loop.time() + ttl_seconds
                        self._current[key] = item
                        self._prev.pop(key, None)
                    break
            # Retry: item was already processed or expired under lock

    async def start(self, ctx: Context) -> None:
        if self._stopped:
            raise StoreStoppedError()
        if self._started:
            raise StoreAlreadyStartedError()
        self._started = True
        if self._config.ttl.total_seconds() > 0:
            self._timer_task = asyncio.create_task(self._rotate())

    async def stop(self, ctx: Context) -> None:
        async with self._rotation_lock.write_lock():
            if self._stopped:
                return
            self._stopped = True
            # Like Go, stop maintenance, not already accepted expiry callbacks.
            timer = self._timer_task
            if timer is not None:
                timer.cancel()
        if timer is not None:
            try:
                await timer
            except asyncio.CancelledError:
                pass


def make_hashmap_storage(env: ServiceEnvironment, cfg: JoinStorageConfig) -> HashMapJoinStorage:
    return HashMapJoinStorage(env, cfg)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Any, Callable, Optional, Hashable, Awaitable
from datetime import datetime

from ..common import ServiceEnvironment
from ..context import Context, request_deadline
from ..environment.metrics import Int64Gauge, Int64Counter
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

        scope = env.metrics.scope('hashmap_join_storage', {
            'service': env.service_config.name,
            'name': cfg.name,
        })
        self._gauge_count = scope.gauge('count', 'Elements count stored in a join storage', None)
        self._evictions_total = scope.counter('evictions_total',
                                              'Total number of items evicted from join storage by TTL', None)

    async def _rotate(self):
        try:
            while True:
                await asyncio.sleep(self._config.ttl.total_seconds())
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
                    if evicted > 0:
                        self._gauge_count.sub(evicted)
                        self._evictions_total.add(evicted)
        except asyncio.CancelledError:
            pass

    async def _after_func(self, key: K, item: Item[V]) -> None:
        """Fires at request deadline: calls callback with accumulated values, then removes item.
        Mirrors context.AfterFunc in Go pools — ensures items don't linger after context expiry.
        """
        try:
            await asyncio.sleep(max(0.0, item.deadline - asyncio.get_running_loop().time()))
        except asyncio.CancelledError:
            return  # join_value processed item before deadline — stopFn equivalent
        async with item.lock:
            if item.processed:
                return
            item.processed = True  # claim execution before releasing lock
        # Call callback outside lock (it may await); item.processed=True prevents re-entry
        if item.callback is not None:
            await item.callback(item.values)
        # Remove from whichever bucket holds the item; decrement gauge once.
        removed = (self._current.pop(key, None) is not None
                   or self._prev.pop(key, None) is not None)
        if removed:
            self._gauge_count.dec()
            self._evictions_total.inc()

    def _make_after_task(self, key: K, item: Item[V]) -> None:
        after_task = asyncio.create_task(self._after_func(key, item))
        item.after_task = after_task
        self._after_tasks.add(after_task)
        after_task.add_done_callback(self._after_tasks.discard)

    async def join_value(self, key: K, index: int, value: V, callback: Callable[[list[list[V]]], Awaitable[bool]]):
        # Mirrors Go's: if ctxDeadline, ok := ctx.Deadline(); ok { ttl = time.Until(ctxDeadline) }
        loop = asyncio.get_running_loop()
        req_deadline = request_deadline.get()
        if req_deadline is not None:
            now = datetime.now(tz=req_deadline.tzinfo)
            remaining = (req_deadline - now).total_seconds()
            ttl_seconds = max(0.0, remaining)
        else:
            ttl_seconds = self._config.ttl.total_seconds()

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
                    item = None

            if item is None:
                # ttl_seconds == 0 → float('inf') mirrors Go's zero time.Time (no individual expiry)
                deadline = (loop.time() + ttl_seconds) if ttl_seconds > 0.0 else float('inf')
                item = Item[V](deadline, index + 1)
                self._current[key] = item
                self._gauge_count.inc()
                # Register after_func for finite deadlines — mirrors context.AfterFunc in Go
                if deadline != float('inf'):
                    self._make_after_task(key, item)

            # Store callback so after_func can invoke it if deadline fires first
            item.callback = callback

            assert item is not None
            async with item.lock:
                now_ts = loop.time()
                if not item.processed and item.deadline > now_ts:
                    if len(item.values) <= index:
                        item.values.extend([[]] * (index - len(item.values) + 1))
                    item.values[index].append(value)
                    item.processed = await callback(item.values)
                    if item.processed:
                        # Cancel after_func — item handled, no need to fire at deadline (stopFn)
                        if item.after_task is not None:
                            item.after_task.cancel()
                        removed = (self._current.pop(key, None) is not None
                                   or self._prev.pop(key, None) is not None)
                        if removed:
                            self._gauge_count.dec()
                    elif self._config.renew_ttl and ttl_seconds > 0.0:
                        # Deadline extended: restart after_func with new deadline
                        if item.after_task is not None:
                            item.after_task.cancel()
                        item.deadline = loop.time() + ttl_seconds
                        self._current[key] = item
                        self._prev.pop(key, None)
                        self._make_after_task(key, item)
                    break
            # Retry: item was already processed or expired under lock

    async def start(self, ctx: Context) -> None:
        if self._stopped:
            raise StoreStoppedError()
        if self._started:
            raise StoreAlreadyStartedError()
        self._started = True
        self._timer_task = asyncio.create_task(self._rotate())

    async def stop(self, ctx: Context) -> None:
        if self._stopped:
            return
        self._stopped = True
        for t in list(self._after_tasks):
            t.cancel()
        self._after_tasks.clear()
        if self._timer_task is not None:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass


def make_hashmap_storage(env: ServiceEnvironment, cfg: JoinStorageConfig) -> HashMapJoinStorage:
    return HashMapJoinStorage(env, cfg)

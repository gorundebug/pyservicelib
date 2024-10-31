#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Any, Callable, Dict, List, Optional, Hashable
from datetime import datetime, timedelta

from pyservicelib.runtime.common import ServiceEnvironment
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.store.storage import JoinStorageConfig, JoinStorage


class Item[V]:
    deadline: datetime
    values: List[List[V]]
    processed: bool
    lock: asyncio.Lock

    def __init__(self, delay: timedelta, min_values: int):
        self.values = [[] for _ in range(min_values)]
        self.deadline = datetime.now() + delay
        self.processed = False
        self.lock = asyncio.Lock()


class HashMapJoinStorage[K: Hashable, V](JoinStorage[K]):

    _config: JoinStorageConfig
    _environment: ServiceEnvironment
    _storage1: Dict[K, Item[V]]
    _storage2: Dict[K, Item[V]]
    _timer_task: Optional[asyncio.Task[Any]]

    def __init__(self, env: ServiceEnvironment, cfg: JoinStorageConfig):
        self._environment = env
        self._config = cfg
        self._storage1 = {}
        self._storage2 = {}
        self._timer_task = None

    async def _start_rotation(self):
        while True:
            await asyncio.sleep(self._config.ttl.total_seconds())
            self._storage2, self._storage1 = self._storage1, {}

    async def join_value(self, key: K, index: int, value: V, callback: Callable[[List[List[V]]], Any]):
        while True:
            storage = self._storage1
            item: Optional[Item[V]] = storage.get(key)

            if item is None or item.deadline >= datetime.now():
                if item is not None:
                    del storage[key]
                storage = self._storage2
                item = storage.get(key)
                if item is not None and item.deadline >= datetime.now():
                    del storage[key]
                    item = None

            if item is None:
                item = Item[V](self._config.ttl, index + 1)
                storage = self._storage1
                storage[key] = item

            async with item.lock:
                if not item.processed and item.deadline < datetime.now():
                    if len(item.values) <= index:
                        item.values.extend([[]] * (index - len(item.values) + 1))
                    item.values[index].append(value)
                    item.processed = await callback(item.values)
                    if item.processed:
                        del storage[key]
                    elif self._config.renew_ttl:
                        if storage is not self._storage1:
                            del storage[key]
                        self._storage1[key] = item
                    break

    async def stop(self, ctx: Context) -> None:
        if self._timer_task is not None:
            self._timer_task.cancel()
            await self._timer_task

    async def start(self, ctx: Context) -> None:
        self._timer_task = asyncio.create_task(self._start_rotation())


def make_hashmap_storage(env: ServiceEnvironment, cfg: JoinStorageConfig):
    return HashMapJoinStorage(env, cfg)
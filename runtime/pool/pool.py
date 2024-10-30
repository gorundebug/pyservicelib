#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from typing import Callable, Any
from datetime import timedelta

from pyservicelib.runtime.context import Context

class Pool(ABC):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass

class DelayPool(Pool):

    @abstractmethod
    async def add_task(self, delay: timedelta, fn: Callable[..., Any], *args, **kwargs):
        pass


class PriorityTaskPool(Pool):

    @abstractmethod
    async def add_task(self, priority: int, fn: Callable[..., Any],  *args, **kwargs):
        pass

class TaskPool(Pool):

    @abstractmethod
    async def add_task(self, fn: Callable[..., Any], *args, **kwargs):
        pass

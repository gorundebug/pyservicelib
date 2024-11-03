#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from typing import Hashable, Callable, Any
from datetime import timedelta

from pyservicelib.runtime.context import Context

class Storage(ABC):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass


class JoinStorageConfig(ABC):

    @property
    @abstractmethod
    def ttl(self) -> timedelta:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def renew_ttl(self) -> bool:
        pass


class JoinStorage[K: Hashable](Storage):

    @abstractmethod
    async def join_value(self, key: K, index: int, value: Any, callback: Callable[[list[list[Any]]], Any]):
        pass
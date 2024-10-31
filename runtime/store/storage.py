#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import datetime
from abc import ABC, abstractmethod
from pyservicelib.runtime.context import Context

class Storage(ABC):

    @abstractmethod
    def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def stop(self, ctx: Context) -> None:
        pass

class JoinStorageConfig(ABC):

    @property
    @abstractmethod
    def ttl(self) -> datetime.timedelta:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def renew_ttl(self) -> bool:
        pass
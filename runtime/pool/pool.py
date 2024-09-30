#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from pyservicelib.runtime.context import Context

class Pool(ABC):

    @abstractmethod
    def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def stop(self, ctx: Context) -> None:
        pass

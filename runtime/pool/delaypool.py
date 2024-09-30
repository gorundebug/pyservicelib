#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import abstractmethod
from pyservicelib.runtime.pool import Pool
from datetime import timedelta
from typing import Callable

class DelayTask:
    pass

class DelayPool(Pool):

    @abstractmethod
    def delay(self, deadline: timedelta, fn: Callable) -> DelayTask:
        pass

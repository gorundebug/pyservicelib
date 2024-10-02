#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import abstractmethod
from pyservicelib.runtime.pool import Pool
from typing import Callable

class PriorityTask:
    pass


class PriorityTaskPool(Pool):

    @abstractmethod
    def add_task(self, priority: int, fn: Callable) -> PriorityTask:
        pass
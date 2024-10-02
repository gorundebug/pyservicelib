#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import abstractmethod
from pyservicelib.runtime.pool import Pool
from typing import Callable

class Task:
    pass


class TaskPool(Pool):

    @abstractmethod
    def add_task(self, fn: Callable) -> Task:
        pass
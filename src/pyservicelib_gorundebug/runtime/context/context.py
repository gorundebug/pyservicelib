#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from datetime import timedelta
import time
from typing import Optional


class Context:
    __deadline: float

    def __init__(self, timeout: Optional[timedelta] = None):
        if timeout is None:
            self.__deadline = float('inf')
        else:
            self.__deadline = time.perf_counter() + timeout.total_seconds()

    @property
    def is_expired(self) -> bool:
        return time.perf_counter() >= self.__deadline

    @property
    def time_left(self) -> Optional[float]:
        if self.__deadline == float('inf'):
            return None
        return max(0.0, self.__deadline - time.perf_counter())

    def bounded(self, timeout: timedelta) -> "Context":
        """Return a child context capped by both this deadline and ``timeout``."""
        seconds = max(0.0, timeout.total_seconds())
        current = self.time_left
        if current is not None:
            seconds = min(seconds, current)
        return Context(timedelta(seconds=seconds))


def default_context() -> Context:
    return Context()

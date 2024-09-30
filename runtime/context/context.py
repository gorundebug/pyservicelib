#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from datetime import timedelta
import time

class Context:
    __deadline: float

    def __init__(self, timeout: timedelta):
        self.__deadline = time.perf_counter() + timeout.total_seconds()

    @property
    def is_expired(self) -> bool:
        return time.perf_counter() >= self.__deadline

    @property
    def time_left(self) -> float:
        return max(0.0, self.__deadline - time.perf_counter())
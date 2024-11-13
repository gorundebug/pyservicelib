#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from threading import Lock

class AtomicInteger:
    __slots__ = ['_value', '_lock']

    _value: int
    _lock: Lock

    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = Lock()

    def inc(self, amount: int = 1):
        with self._lock:
            self._value += amount
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import re

_re_split = re.compile(r'[A-Z][^A-Z\s_-]*|[^A-Z\s_-]+')


def to_snake_case(text: str) -> str:
    words = _re_split.findall(text)
    return '_'.join(w.lower() for w in words)


class Collection[T]:
    _data: list[T]

    def __init__(self, data: list[T]):
        self._data = data

    def len(self) -> int:
        return len(self._data)

    def at(self, i: int) -> T:
        return self._data[i]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

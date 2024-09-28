#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from dataclasses import dataclass
from collections.abc import Hashable

@dataclass
class KeyValue[K: Hashable, V]:
    key: K
    value: V
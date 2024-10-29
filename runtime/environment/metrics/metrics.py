#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Callable
from abc import ABC, abstractmethod

MetricsHandler = Callable[[], bytes]

class Metrics(ABC):
    pass

class MetricsEngine(ABC):
    @property
    @abstractmethod
    def metrics(self) -> Metrics:
        pass

    @property
    @abstractmethod
    def metrics_handler(self) -> MetricsHandler:
        pass
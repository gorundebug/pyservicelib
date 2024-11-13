#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from typing import Optional

from ..config import ServiceAppConfig, ServiceConfig
from .log import Logger, LogsEngine
from .metrics import Metrics, MetricsEngine

class ServiceEnvironment(ABC):

    @property
    @abstractmethod
    def config(self) -> ServiceAppConfig:
        pass

    @property
    @abstractmethod
    def service_config(self) -> ServiceConfig:
        pass

    @property
    @abstractmethod
    def log(self) -> Logger:
        pass

    @property
    @abstractmethod
    def metrics(self) -> Metrics:
        pass

    @property
    @abstractmethod
    def service_dependency(self) -> Optional["ServiceDependency"]:
        pass


class ServiceDependency(ABC):

    @abstractmethod
    async def get_logs_engine(self, env: ServiceEnvironment) -> Optional[LogsEngine]:
        pass;

    @abstractmethod
    async def get_metrics_engine(self, env: ServiceEnvironment) -> Optional[MetricsEngine]:
        pass;



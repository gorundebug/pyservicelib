#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from typing import Optional, Protocol, runtime_checkable

from ..config import ServiceAppConfig, ServiceConfig
from ..context import Context
from .log import Logger, LogsEngine
from .metrics import Metrics, MetricsEngine
from .tracing import Tracing, TracingEngine


@runtime_checkable
class Lifecycle(Protocol):
    async def start(self, ctx: Context) -> None: ...
    async def stop(self, ctx: Context) -> None: ...


@runtime_checkable
class AdmissionLifecycle(Lifecycle, Protocol):
    """A component whose inbound admission stops before graph draining."""

    async def stop_admission(self, ctx: Context) -> None: ...


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
    def tracing(self) -> Optional[Tracing]:
        pass

    @property
    @abstractmethod
    def service_dependency(self) -> Optional["ServiceDependency"]:
        pass


class ServiceDependency(ABC):

    @abstractmethod
    async def get_logs_engine(self, env: ServiceEnvironment) -> Optional[LogsEngine]:
        pass

    @abstractmethod
    async def get_metrics_engine(self, env: ServiceEnvironment) -> Optional[MetricsEngine]:
        pass

    async def get_tracing_engine(self, env: ServiceEnvironment) -> Optional[TracingEngine]:
        return None

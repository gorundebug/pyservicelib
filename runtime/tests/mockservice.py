#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Optional

from pyservicelib.runtime.environment import ServiceDependency, ServiceEnvironment
from pyservicelib.runtime.environment.log import LogsEngine
from pyservicelib.runtime.environment.metrics import MetricsEngine
from pyservicelib.runtime.serviceapp import ServiceApp
from pyservicelib.runtime.config import ServiceAppConfig


class MockServiceConfig(ServiceAppConfig):
    def __init__(self, **data):
        super().__init__(**data)

    @classmethod
    def load_config(cls, obj: Optional[dict[str, Any]]) -> dict[str, Any]:
        return {}

class MockServiceDependency(ServiceDependency):
    async def get_logs_engine(self, env: ServiceEnvironment) -> Optional[LogsEngine]:
        return None

    async def get_metrics_engine(self, env: ServiceEnvironment) -> Optional[MetricsEngine]:
        return None


class MockService(ServiceApp):

    def __init__(self):
        super().__init__()

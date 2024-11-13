#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Optional
from pydantic import BaseModel

from pyservicelib_gorundebug.runtime.environment import ServiceDependency, ServiceEnvironment
from pyservicelib_gorundebug.runtime.environment.log import LogsEngine
from pyservicelib_gorundebug.runtime.environment.metrics import MetricsEngine
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp
from pyservicelib_gorundebug.runtime.config import ServiceAppConfig

class RequestData(BaseModel):
    param1: Optional[str] = None
    param2: Optional[int] = None


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

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Dict, Optional, cast
import os
from pathlib import Path

from pyservicelib.runtime.environment import ServiceDependency, ServiceEnvironment
from pyservicelib.runtime.environment.log import LogsEngine
from pyservicelib.runtime.environment.metrics import MetricsEngine
from pyservicelib.runtime.serde import Serde
from pyservicelib.runtime.serviceapp import ServiceApp, ServiceAppLoader
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.config import ServiceAppConfig, ConfigSettings


class MockServiceConfig(ServiceAppConfig):
    def __init__(self, **data):
        super().__init__(**data)

    @classmethod
    def load_config(cls, obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return {}

class MockServiceDependency(ServiceDependency):
    async def get_logs_engine(self, env: ServiceEnvironment) -> Optional[LogsEngine]:
        return None

    async def get_metrics_engine(self, env: ServiceEnvironment) -> Optional[MetricsEngine]:
        return None


class MockService(ServiceApp):

    def __init__(self):
        super().__init__()

async def test_serde_type_dict():
    os.chdir(Path(__file__).parent)
    value = {1: True, 2: False, 3: True}

    service = await ServiceAppLoader[MockService, MockServiceConfig]().init("MockService", MockServiceDependency(), ConfigSettings())
    ctx = default_context()
    ser = cast(Serde[dict[int, bool]], service.get_type_serde("MapType"))

    b = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(b)

    await service.stop(ctx)
    await service.release()
    assert value == value_copy

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Dict, Optional
import os
from pathlib import Path
import pytest

from pyservicelib.runtime.serviceapp import ServiceApp, ServiceLoader
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.config import ServiceAppConfig, ConfigSettings


class MockServiceConfig(ServiceAppConfig):
    def __init__(self, **data):
        super().__init__(**data)

    @classmethod
    def load_config(cls, obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return {}


class MockService(ServiceApp):

    def __init__(self):
        super().__init__()

@pytest.mark.asyncio(loop_scope="function")
async def test_serde_type_dict():
    os.chdir(Path(__file__).parent)

    service = await ServiceLoader[MockService, MockServiceConfig]().init("MockService", ConfigSettings())
    ctx = default_context()

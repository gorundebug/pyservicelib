#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Any, Dict, Optional, cast
import os
from pathlib import Path

from pyservicelib.runtime import Serde
from pyservicelib.runtime.serviceapp import ServiceApp, ServiceAppLoader
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

async def test_serde_type_dict():
    os.chdir(Path(__file__).parent)
    value = {1: True, 2: False, 3: True}

    service = await ServiceAppLoader[MockService, MockServiceConfig]().init("MockService", ConfigSettings())
    ctx = default_context()
    ser = cast(Serde[dict[int, bool]], service.get_type_serde("MapType"))

    b = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(b)

    await service.stop(ctx)
    assert value == value_copy

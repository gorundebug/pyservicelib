#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import cast
import os
from pathlib import Path


from pyservicelib.runtime.serde import Serde
from pyservicelib.runtime.serviceapp import  ServiceAppLoader
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.config import  ConfigSettings
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency


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

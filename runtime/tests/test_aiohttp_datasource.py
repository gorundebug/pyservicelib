#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import os
from pathlib import Path
import pytest

from pyservicelib import transformation
from pyservicelib.datasource.http.aiohttpds import AIOHttpEndpointConsumer
from pyservicelib.runtime.config import ConfigSettings
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.serviceapp import ServiceAppLoader
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency


@pytest.mark.asyncio
async def test_serde_type_dict():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("MockService",
                                                                            MockServiceDependency(),
                                                                            ConfigSettings())
    stream = transformation.Input[int]("Input", service)

    ec = AIOHttpEndpointConsumer(stream)

    ctx = default_context()

    await service.stop(ctx)
    await service.release()
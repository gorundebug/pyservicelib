#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import asyncio
import os
from pathlib import Path
import pytest
from typing import Optional
from pydantic import BaseModel

from pyservicelib import transformation
from pyservicelib.datasource.http.aiohttpds import AIOHttpEndpointConsumer
from pyservicelib.runtime.config import ConfigSettings
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.serviceapp import ServiceAppLoader
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency

class RequestData(BaseModel):
    param1: Optional[str] = None
    param2: Optional[int] = None

@pytest.mark.asyncio
async def test_aiohttp_datasource():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("MockService",
                                                                            MockServiceDependency(),
                                                                            ConfigSettings())
    stream = transformation.Input[RequestData]("Input", service)

    ec = AIOHttpEndpointConsumer[RequestData](stream)

    ctx = default_context()
    await service.start(ctx)

    await asyncio.sleep(20)

    await service.stop(ctx)
    await service.release()
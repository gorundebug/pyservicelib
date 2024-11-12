#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import json
import os
from pathlib import Path
import aiohttp
import pytest
from typing import Optional

from pyservicelib import transformation
from pyservicelib.datasource.http.aiohttpds import AIOHttpEndpointConsumer
from pyservicelib.runtime import Consume
from pyservicelib.runtime.config import ConfigSettings
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.serviceapp import ServiceAppLoader
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency, RequestData

class MockServiceRequestDataConsumer(Consume[RequestData]):
    _request_data: Optional[RequestData]
    _service: MockService

    def __init__(self, service: MockService):
        self._service = service
        self._request_data = None

    async def consume(self, value: RequestData) -> None:
        self._request_data = value

    @property
    def request_data(self) -> Optional[RequestData]:
        return self._request_data


async def make_request(url, payload) -> int:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            await response.read()
            return response.status


@pytest.mark.asyncio
async def test_aiohttp_datasource():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("MockService",
                                                                            MockServiceDependency(),
                                                                            ConfigSettings())
    input_stream = transformation.Input[RequestData]("Input", service)
    rd_consumer = MockServiceRequestDataConsumer(service)
    appsink_stream = transformation.AppSink[RequestData]("AppSink", input_stream, rd_consumer)
    _ = appsink_stream

    ec = AIOHttpEndpointConsumer[RequestData](input_stream)

    ctx = default_context()
    await service.start(ctx)

    payload = {
        "param1": "value1",
        "param2": 42
    }

    status = await make_request("http://127.0.0.1:8081/", json.dumps(payload))
    assert status == 200
    assert rd_consumer.request_data is not None
    assert rd_consumer.request_data.param1 == "value1"
    assert rd_consumer.request_data.param2 == 42

    await service.stop(ctx)
    await service.release()
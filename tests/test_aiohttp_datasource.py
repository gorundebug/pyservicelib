#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import json
import os
from pathlib import Path
from typing import Optional

import aiohttp
import pytest
from aiohttp import web

from pyservicelib_gorundebug.datasource.http.aiohttpds import (
    HandlerData, EndpointHandler, ResultContext, make_net_http_endpoint_consumer,
)
from pyservicelib_gorundebug.runtime.common import StreamContext, TypedInputStream
from pyservicelib_gorundebug.runtime.config import ConfigSettings
from pyservicelib_gorundebug.runtime.context import default_context
from pyservicelib_gorundebug.runtime.serviceapp import ServiceAppLoader

from .mockservice import MockService, MockServiceConfig, MockServiceDependency, RequestData


class RequestDataHandler:
    """EndpointHandler that parses JSON body into RequestData and pushes it into the pipeline."""

    async def begin_request(
        self,
        sc: StreamContext,
        data: HandlerData,
    ) -> tuple[HandlerData, None, Optional[Exception]]:
        return data, None, None

    async def consume_message(
        self,
        sc: StreamContext,
        handler_state: None,
        data: HandlerData,
        result_ctx: ResultContext,
    ) -> Optional[Exception]:
        try:
            body = await data.request.json()
            value = RequestData(**body)
            await sc.collect(value)
            data.set_response(web.Response(status=200))
        except Exception as e:
            data.set_response(web.Response(status=400, text=str(e)))
            return e
        return None

    def get_message_id(self, sc: StreamContext, handler_state: None, value: object) -> str:
        return ""

    async def end_request(
        self,
        sc: StreamContext,
        err: Optional[Exception],
        handler_state: None,
        data: HandlerData,
    ) -> None:
        if err is not None and not data._response.done():
            data.set_response(web.Response(status=500, text=str(err)))


class MockServiceRequestDataConsumer:
    received: Optional[RequestData] = None

    async def consume(self, value: RequestData) -> None:
        self.received = value


async def make_request(url: str, payload: dict) -> int:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            await response.read()
            return response.status


@pytest.mark.asyncio
async def test_aiohttp_datasource():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load(
        "MockService", MockServiceDependency(), ConfigSettings()
    )

    # TODO: wire up stream + endpoint via config and make_net_http_endpoint_consumer
    # This test requires a proper config file with HttpDataConnectorConfig.
    # Skipping until config fixtures are available.
    pytest.skip("requires HttpDataConnectorConfig fixture")

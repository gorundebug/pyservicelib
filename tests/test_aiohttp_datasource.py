#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import asyncio
from typing import Optional

import aiohttp
import pytest
from aiohttp import web

from pyservicelib_gorundebug.datasource.http.aiohttpds import (
    HandlerData, ResultContext,
)
from pyservicelib_gorundebug.runtime.common import StreamContext

from .mockservice import RequestData, setup, teardown


class RequestDataHandler:
    """EndpointHandler that parses JSON body into RequestData and pushes it into the pipeline."""

    async def begin_request(
        self,
        sc: StreamContext,
        data: HandlerData,
    ) -> tuple[HandlerData, None]:
        return data, None

    async def consume_message(
        self,
        sc: StreamContext,
        handler_state: None,
        data: HandlerData,
        result_ctx: ResultContext,
    ) -> None:
        try:
            body = await data.request.json()
            value = RequestData(**body)
            await sc.collect(value)
            data.set_response(web.Response(status=200))
        except Exception as e:
            data.set_response(web.Response(status=400, text=str(e)))
            raise

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
    env = await setup()
    try:
        event_waiter = asyncio.create_task(env.service.request_data_event.wait())
        status = await make_request("http://127.0.0.1:9091/data", {"text": "hello"})
        assert status == 200
        await asyncio.wait_for(event_waiter, timeout=5.0)
        assert env.service.request_data is not None
        assert env.service.request_data.text == "hello"
    finally:
        await teardown(env)

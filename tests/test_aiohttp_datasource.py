#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import asyncio
from types import SimpleNamespace
from typing import Optional

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from pyservicelib_gorundebug.datasource.http.aiohttpds import (
    HandlerData, ResultContext, _NetHTTPTypedEndpointConsumer,
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


async def make_text_request(url: str, payload: dict) -> tuple[int, str, str]:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            return (
                response.status,
                response.headers.get("Content-Type", ""),
                await response.text(),
            )


async def get_metrics(url: str) -> tuple[int, str, str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return (
                response.status,
                response.headers["Content-Type"],
                await response.text(),
            )


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

        error_status, error_content_type, error_body = await make_text_request(
            "http://127.0.0.1:9091/data", {"text": "client-error"}
        )
        assert error_status == 400
        assert error_content_type == "text/plain; charset=utf-8"
        assert error_body == "bad request\n"

        metrics_status, content_type, metrics = await get_metrics(
            "http://127.0.0.1:9091/metrics"
        )
        assert metrics_status == 200
        assert content_type == "text/plain; version=0.0.4; charset=utf-8"
        assert "# HELP" in metrics
    finally:
        await teardown(env)


@pytest.mark.asyncio
async def test_result_endpoint_returns_response_set_by_handler():
    class ImmediateResponseHandler:
        async def begin_request(self, sc, data):
            return data, None

        async def consume_message(self, sc, handler_state, data, result_ctx):
            data.set_response(web.Response(status=201, text="created"))
            result_ctx.done()

        async def end_request(self, sc, err, handler_state, data):
            pass

    endpoint = SimpleNamespace(
        name="ResultEndpoint",
        on_request_start=lambda: 0.0,
        on_request_end=lambda *args: None,
        on_begin_request_failed=lambda err: None,
        on_pending_add=lambda stream_id: None,
        on_pending_remove=lambda stream_id: None,
    )
    consumer = object.__new__(_NetHTTPTypedEndpointConsumer)
    consumer._handler = ImmediateResponseHandler()
    consumer._sc = SimpleNamespace()
    consumer._endpoint = endpoint
    consumer._input_stream = SimpleNamespace(name="ResultInput")
    consumer._tracer = None
    consumer._has_result = True
    consumer._pending = None
    consumer._method = "POST"
    consumer._path = "/result"

    request = make_mocked_request("POST", "/result")
    response = await consumer._serve_http(request)

    assert response.status == 201
    assert response.text == "created"

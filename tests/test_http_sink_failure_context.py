from types import SimpleNamespace
from typing import Any, cast

import aiohttp
import pytest

from pyservicelib_gorundebug.datasink.http.aiohttpds import (
    Requester, Response, _AIOHttpSinkEndpoint, _NetHTTPSinkEndpointConsumer,
)
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.runtime.context.request import request_stream_id

from .test_grpc_sink_streaming import _FakeStream


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ["begin", "request"])
@pytest.mark.parametrize("metrics_enabled", [False, True])
async def test_http_failure_preserves_parent_context_and_finalizes_metrics(
    monkeypatch: pytest.MonkeyPatch, failure_phase: str, metrics_enabled: bool,
) -> None:
    failure = RuntimeError(f"{failure_phase} failed")
    terminal_ids: list[str | None] = []
    transport_ids: list[str | None] = []
    terminal_errors: list[Exception | None] = []
    metrics_calls: list[tuple[Any, ...]] = []
    begin_errors: list[Exception] = []

    class Endpoint:
        name = "HTTP endpoint"
        environment = SimpleNamespace(metrics=SimpleNamespace(enabled=metrics_enabled))
        # execute() is replaced below, so no real session or network is needed.
        datasink = SimpleNamespace(session=object())

        def add_endpoint_consumer(self, consumer: object) -> None:
            pass

        def on_request_start(self) -> float:
            return 0.0

        def on_request_end(self, *args: Any) -> None:
            metrics_calls.append(args)

        def on_begin_request_failed(self, err: Exception) -> None:
            begin_errors.append(err)

    class Handler:
        async def begin_request(self, sc: object) -> None:
            if failure_phase == "begin":
                raise failure

        async def consume_message(
            self, sc: object, handler_state: None, value: object, req: Requester,
        ) -> None:
            req.new_request("POST", "http://unused.invalid/request", data=b"request")

        async def handle_response(
            self, sc: object, handler_state: None, resp: Response,
        ) -> None:
            raise AssertionError("failed request must not produce a response")

        async def end_request(
            self, sc: object, err: Exception | None, handler_state: None,
        ) -> None:
            terminal_ids.append(request_stream_id.get())
            terminal_errors.append(err)

    async def execute(self: Requester) -> aiohttp.ClientResponse:
        transport_ids.append(request_stream_id.get())
        assert self._kwargs["headers"]["x-stream-id"] == request_stream_id.get()
        raise failure

    monkeypatch.setattr(Requester, "execute", execute)
    consumer = _NetHTTPSinkEndpointConsumer(
        cast(_AIOHttpSinkEndpoint, Endpoint()),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        Handler(), None, None,
    )
    token = request_stream_id.set("parent-request")
    try:
        await consumer.consume("request")
        assert request_stream_id.get() == "parent-request"
        if failure_phase == "begin":
            assert begin_errors == [failure]
            assert not terminal_errors
            assert not transport_ids
        else:
            assert not begin_errors
            assert terminal_ids == ["parent-request"]
            assert terminal_errors == [failure]
            assert len(transport_ids) == 1
            assert transport_ids[0] not in (None, "parent-request")
        assert len(metrics_calls) == 1
        assert metrics_calls[0][1] is failure
        expected_body_size = 7 if metrics_enabled and failure_phase == "request" else None
        assert metrics_calls[0][3] == expected_body_size
    finally:
        request_stream_id.reset(token)

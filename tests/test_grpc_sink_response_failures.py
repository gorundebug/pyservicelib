import asyncio
from typing import Any, cast

import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _GrpcSinkEndpoint,
    _NoStreamingSinkConsumer,
    _ServerStreamingSinkConsumer,
)
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.runtime.context.request import request_stream_id

from .test_grpc_sink_streaming import _FakeEndpoint, _FakeStream, _RecordingHandler


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["unary", "server"])
async def test_open_failure_restores_parent_id_before_end_request(mode: str) -> None:
    terminal_ids: list[str | None] = []
    transport_ids: list[str | None] = []

    class Handler(_RecordingHandler):
        async def end_request(self, sc, err, handler_state) -> None:
            terminal_ids.append(request_stream_id.get())
            await super().end_request(sc, err, handler_state)

    async def unary_client(*args: Any, **kwargs: Any) -> Any:
        transport_ids.append(request_stream_id.get())
        raise RuntimeError("open failed")

    def server_client(*args: Any, **kwargs: Any) -> Any:
        transport_ids.append(request_stream_id.get())
        raise RuntimeError("open failed")

    handler = Handler()
    endpoint = cast(_GrpcSinkEndpoint, _FakeEndpoint())
    stream = cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream())
    consumer = (
        _NoStreamingSinkConsumer(endpoint, stream, handler, None, None, unary_client)
        if mode == "unary" else
        _ServerStreamingSinkConsumer(endpoint, stream, handler, None, None, server_client)
    )
    token = request_stream_id.set("parent-request")
    try:
        await asyncio.wait_for(consumer.consume("request"), 1)
        assert len(transport_ids) == 1
        assert transport_ids[0] not in (None, "parent-request")
        assert terminal_ids == ["parent-request"]
        assert len(handler.end_calls) == 1
        assert str(handler.end_calls[0]) == "open failed"
        assert request_stream_id.get() == "parent-request"
    finally:
        request_stream_id.reset(token)


@pytest.mark.asyncio
async def test_server_receive_failure_finalizes_after_delivered_responses() -> None:
    handler = _RecordingHandler()

    class Responses:
        async def __aiter__(self):
            yield "first"
            raise RuntimeError("receive failed")

    def client_fn(*args: Any, **kwargs: Any) -> Any:
        return Responses()

    consumer = _ServerStreamingSinkConsumer(
        cast(_GrpcSinkEndpoint, _FakeEndpoint()),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        handler, None, None, client_fn,
    )
    token = request_stream_id.set("parent-request")
    try:
        await asyncio.wait_for(consumer.consume("request"), 1)
        assert handler.responses == ["first"]
        assert len(handler.end_calls) == 1
        assert str(handler.end_calls[0]) == "receive failed"
        assert request_stream_id.get() == "parent-request"
    finally:
        request_stream_id.reset(token)

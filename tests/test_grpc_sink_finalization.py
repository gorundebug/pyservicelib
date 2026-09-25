from typing import cast
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.datasink.grpc.grpcds import _GrpcSinkEndpoint
import asyncio
from typing import Any

import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _BidiStreamingSinkConsumer,
    _ClientStreamingSinkConsumer,
)
from pyservicelib_gorundebug.runtime.context.request import request_stream_id

from .test_grpc_sink_streaming import (
    _FakeBidiStreamingCall,
    _FakeClientStreamingCall,
    _FakeEndpoint,
    _FakeStream,
    _RecordingHandler,
)


@pytest.mark.asyncio
async def test_client_done_starts_receive_before_active_message_finishes() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    half_closed = asyncio.Event()
    finished = asyncio.Event()

    class Call(_FakeClientStreamingCall):
        async def done_writing(self) -> None:
            await super().done_writing()
            half_closed.set()

    class Handler(_RecordingHandler):
        async def consume_message(self, sc, handler_state, value, sender, result_ctx) -> None:
            await super().consume_message(sc, handler_state, value, sender, result_ctx)
            entered.set()
            await release.wait()

        async def end_request(self, sc, err, handler_state) -> None:
            await super().end_request(sc, err, handler_state)
            finished.set()

    call = Call(response="response")
    handler = Handler(done_after=1)

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Call:
        return call

    consumer = _ClientStreamingSinkConsumer(
        cast(_GrpcSinkEndpoint, _FakeEndpoint()), cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()), handler, None, None, client_fn,
    )
    token = request_stream_id.set("active-message")
    task = asyncio.create_task(consumer.consume("request"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(half_closed.wait(), 1)
        assert not handler.responses
        assert not finished.is_set()
    finally:
        release.set()
        await asyncio.wait_for(task, 1)
        await asyncio.wait_for(finished.wait(), 1)
        request_stream_id.reset(token)
    assert handler.responses == ["response"]
    assert handler.end_calls == [None]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["client", "bidi"])
async def test_stream_id_reserved_through_reentrant_end_request(mode: str) -> None:
    finished = asyncio.Event()
    opening_count = 0
    endpoint = _FakeEndpoint()

    class Handler(_RecordingHandler):
        async def end_request(self, sc, err, handler_state) -> None:
            # A closing ID must be rejected without waiting for this very
            # finalizer, opening another RPC, or running another callback.
            if not finished.is_set():
                finished.set()
                await consumer.consume("reentrant")
            await super().end_request(sc, err, handler_state)

    handler = Handler(done_after=1)

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Any:
        nonlocal opening_count
        opening_count += 1
        if mode == "client":
            return _FakeClientStreamingCall(response="response")
        return _FakeBidiStreamingCall(responses=["response"])

    consumer_type = (
        _ClientStreamingSinkConsumer if mode == "client"
        else _BidiStreamingSinkConsumer
    )
    consumer = consumer_type(cast(_GrpcSinkEndpoint, endpoint), cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()), handler, None, None, client_fn)
    token = request_stream_id.set("finalizing-message")
    try:
        await consumer.consume("request")
        await asyncio.wait_for(finished.wait(), 1)
        # Drain scheduled completions so a wrongly opened second RPC cannot
        # escape the assertions or survive past the test loop.
        for _ in range(10):
            await asyncio.sleep(0)
        assert opening_count == 1
        assert handler.consume_calls == ["request"]
        assert handler.end_calls == [None]
        assert len(endpoint.begin_failures) == 1
        _, found = consumer._pending.get("finalizing-message")
        assert not found
    finally:
        request_stream_id.reset(token)

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _ClientStreamingSinkConsumer, _GrpcSinkEndpoint,
)
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled, request_deadline, request_stream_id,
)

from .test_grpc_sink_streaming import (
    _FakeClientStreamingCall, _FakeEndpoint, _FakeStream, _RecordingHandler,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["cancelled", "deadline"])
async def test_context_completion_retains_active_message_and_stream_id(reason: str) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    cancelled = asyncio.Event()
    endpoint = _FakeEndpoint()
    call = _FakeClientStreamingCall(response="must not be delivered")
    openings = 0

    class Handler(_RecordingHandler):
        result_context: Any = None

        async def consume_message(self, sc, handler_state, value, sender, result_ctx):
            self.result_context = result_ctx
            await super().consume_message(sc, handler_state, value, sender, result_ctx)
            entered.set()
            await release.wait()

        async def end_request(self, sc, err, handler_state):
            assert release.is_set(), "EndRequest ran before ConsumeMessage completed"
            await super().end_request(sc, err, handler_state)
            finished.set()

    handler = Handler(done_after=2)

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Any:
        nonlocal openings
        openings += 1
        return call

    consumer = _ClientStreamingSinkConsumer(
        cast(_GrpcSinkEndpoint, endpoint),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        handler, None, None, client_fn,
    )
    stream_token = request_stream_id.set("cancelled-active-message")
    cancel_token = request_cancelled.set(cancelled)
    deadline_token = request_deadline.set(
        datetime.now(timezone.utc) + timedelta(milliseconds=100)
        if reason == "deadline" else None
    )
    task = asyncio.create_task(consumer.consume("first"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if reason == "cancelled":
            cancelled.set()

        async def wait_until_closing():
            while True:
                cell, reserved = consumer._pending.get("cancelled-active-message")
                assert reserved, "active message lost its reservation"
                assert cell is not None
                if cell.closing:
                    return
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_until_closing(), 1)
        assert not finished.is_set()
        assert not handler.end_calls
        await asyncio.wait_for(consumer.consume("rejected"), 1)
        assert openings == 1
        assert handler.consume_calls == ["first"]
        assert len(endpoint.begin_failures) == 1
        assert not call.done_writing_called
    finally:
        release.set()
        # Release Done as well if the implementation regresses to ignoring cancellation.
        if handler.result_context is not None:
            handler.result_context.done()
        try:
            await asyncio.wait_for(task, 1)
            await asyncio.wait_for(finished.wait(), 1)
        finally:
            request_deadline.reset(deadline_token)
            request_cancelled.reset(cancel_token)
            request_stream_id.reset(stream_token)

    assert len(handler.end_calls) == 1
    assert str(handler.end_calls[0]) == (
        "request cancelled" if reason == "cancelled" else "deadline exceeded"
    )
    assert not handler.responses
    _, reserved = consumer._pending.get("cancelled-active-message")
    assert not reserved

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
async def test_client_stream_context_completes_without_user_done(reason: str) -> None:
    finished = asyncio.Event()
    transport_closed = asyncio.Event()
    response_requested = asyncio.Event()
    cancelled = asyncio.Event()

    class Call(_FakeClientStreamingCall):
        async def done_writing(self) -> None:
            await super().done_writing()
            transport_closed.set()

        def __await__(self):
            async def response():
                response_requested.set()
                await transport_closed.wait()
                raise AssertionError("cancelled requests must not call CloseAndRecv")
            return response().__await__()

    class Handler(_RecordingHandler):
        result_context: Any = None

        async def consume_message(self, sc, handler_state, value, sender, result_ctx):
            self.result_context = result_ctx
            await super().consume_message(sc, handler_state, value, sender, result_ctx)

        async def end_request(self, sc, err, handler_state):
            await super().end_request(sc, err, handler_state)
            finished.set()

    call = Call()
    handler = Handler(done_after=2)

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Any:
        if reason == "deadline":
            assert timeout is not None
        return call

    consumer = _ClientStreamingSinkConsumer(
        cast(_GrpcSinkEndpoint, _FakeEndpoint()),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        handler, None, None, client_fn,
    )
    stream_token = request_stream_id.set("context-completion")
    cancel_token = request_cancelled.set(cancelled)
    deadline_token = request_deadline.set(
        datetime.now(timezone.utc) + timedelta(milliseconds=50)
        if reason == "deadline" else None
    )
    try:
        await consumer.consume("request")
        if reason == "cancelled":
            cancelled.set()
        await asyncio.wait_for(finished.wait(), 0.75)
        assert not transport_closed.is_set()
        assert not response_requested.is_set()
        assert len(handler.end_calls) == 1
        error = handler.end_calls[0]
        if reason == "cancelled":
            assert isinstance(error, RuntimeError)
            assert str(error) == "request cancelled"
        else:
            assert isinstance(error, TimeoutError)
            assert str(error) == "deadline exceeded"
        assert not handler.responses
        _, reserved = consumer._pending.get("context-completion")
        assert not reserved
    finally:
        # Also release the previous implementation after a regression timeout.
        if handler.result_context is not None:
            handler.result_context.done()
        await asyncio.wait_for(finished.wait(), 1)
        request_deadline.reset(deadline_token)
        request_cancelled.reset(cancel_token)
        request_stream_id.reset(stream_token)

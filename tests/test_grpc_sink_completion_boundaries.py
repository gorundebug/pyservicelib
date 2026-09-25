import asyncio
from typing import Any, cast

import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _BidiStreamingSinkConsumer, _ClientStreamingSinkConsumer, _GrpcSinkEndpoint,
)
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.runtime.context.request import request_stream_id

from .test_grpc_sink_streaming import (
    _FakeBidiStreamingCall, _FakeEndpoint, _FakeStream, _RecordingHandler,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("receive_error", [False, True])
async def test_bidi_receive_completion_does_not_require_done(receive_error: bool) -> None:
    finished = asyncio.Event()
    send_closed = asyncio.Event()
    endpoint = _FakeEndpoint()

    class Call(_FakeBidiStreamingCall):
        async def done_writing(self) -> None:
            await super().done_writing()
            send_closed.set()

        async def _iter(self):
            yield "response"
            if receive_error:
                raise RuntimeError("receive failed")

    class Handler(_RecordingHandler):
        result_context: Any = None

        async def consume_message(self, sc, handler_state, value, sender, result_ctx):
            self.result_context = result_ctx
            await super().consume_message(sc, handler_state, value, sender, result_ctx)

        async def end_request(self, sc, err, handler_state) -> None:
            await super().end_request(sc, err, handler_state)
            finished.set()

    # Only one message is submitted: Done is deliberately never requested.
    handler = Handler(done_after=2)
    call = Call()

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Any:
        return call

    consumer = _BidiStreamingSinkConsumer(
        cast(_GrpcSinkEndpoint, endpoint),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        handler, None, None, client_fn,
    )
    token = request_stream_id.set("receive-completed")
    try:
        await consumer.consume("request")
        await asyncio.wait_for(finished.wait(), 0.5)
        assert handler.responses == ["response"]
        assert len(handler.end_calls) == 1
        if receive_error:
            assert str(handler.end_calls[0]) == "receive failed"
        else:
            assert handler.end_calls == [None]
        await asyncio.wait_for(send_closed.wait(), 0.5)
        assert call.done_writing_called
        _, reserved = consumer._pending.get("receive-completed")
        assert not reserved
    finally:
        # Release the old implementation as well when the regression fails.
        if handler.result_context is not None:
            handler.result_context.done()
        await asyncio.wait_for(finished.wait(), 1)
        request_stream_id.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["client", "bidi"])
async def test_failed_open_reentrant_end_request_rejects_without_waiting(mode: str) -> None:
    endpoint = _FakeEndpoint()
    openings = 0
    reentered = False

    class Handler(_RecordingHandler):
        async def end_request(self, sc, err, handler_state) -> None:
            nonlocal reentered
            if not reentered:
                reentered = True
                await consumer.consume("reentrant")
            await super().end_request(sc, err, handler_state)

    handler = Handler()

    async def client_fn(metadata: Any = (), timeout: float | None = None) -> Any:
        nonlocal openings
        openings += 1
        raise RuntimeError("open failed")

    consumer_type = _ClientStreamingSinkConsumer if mode == "client" else _BidiStreamingSinkConsumer
    consumer = consumer_type(
        cast(_GrpcSinkEndpoint, endpoint),
        cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
        handler, None, None, client_fn,
    )
    token = request_stream_id.set("failed-open")
    try:
        await asyncio.wait_for(consumer.consume("request"), 0.5)
        assert reentered
        assert openings == 1
        assert len(handler.end_calls) == 1
        assert str(handler.end_calls[0]) == "open failed"
        assert not handler.consume_calls
        assert len(endpoint.begin_failures) >= 1
        _, reserved = consumer._pending.get("failed-open")
        assert not reserved
    finally:
        request_stream_id.reset(token)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

"""
Tests for _ClientStreamingSinkConsumer / _BidiStreamingSinkConsumer in
datasink/grpc/grpcds.py — specifically the Go-parity behavior added here:
multiple Consume() calls sharing the same stream_id must reuse a single
underlying gRPC stream (one begin_request, one client_fn call, N
consume_message calls), rather than each Consume() opening and closing its
own independent stream.
"""

import asyncio
import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _ClientStreamingSinkConsumer, _BidiStreamingSinkConsumer,
)
from pyservicelib_gorundebug.runtime.context.request import with_stream_id


class _FakeErrorStream:
    async def consume(self, value):
        pass


class _FakeStream:
    def __init__(self, name="test-stream"):
        self.name = name
        self.error_stream = _FakeErrorStream()
        self.consumed_results = []

    async def consume_result(self, value):
        self.consumed_results.append(value)

    def set_sink_consumer(self, consumer):
        pass


class _FakeEndpoint:
    def __init__(self, name="test-endpoint"):
        self.name = name
        self.begin_failures = []
        self.late_results = []
        self.request_ends = []

    def add_endpoint_consumer(self, consumer):
        pass

    def on_begin_request_failed(self, err):
        self.begin_failures.append(err)

    def on_late_result(self, stream_id):
        self.late_results.append(stream_id)

    def on_request_start(self):
        return 0.0

    def on_request_end(self, start_time, err):
        self.request_ends.append(err)


class _FakeClientStreamingCall:
    """Fake grpc.aio client-streaming call object."""

    def __init__(self, response=None, response_error=None):
        self.writes = []
        self.done_writing_called = False
        self._response = response
        self._response_error = response_error

    async def write(self, req):
        self.writes.append(req)

    async def done_writing(self):
        self.done_writing_called = True

    def __await__(self):
        async def _get():
            if self._response_error is not None:
                raise self._response_error
            return self._response
        return _get().__await__()


class _FakeBidiStreamingCall:
    """Fake grpc.aio bidi-streaming call object."""

    def __init__(self, responses=()):
        self.writes = []
        self.done_writing_called = False
        self._responses = list(responses)

    async def write(self, req):
        self.writes.append(req)

    async def done_writing(self):
        self.done_writing_called = True

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for r in self._responses:
            yield r


class _RecordingHandler:
    """Fake EndpointHandler recording every call. consume_message() calls
    done() once handler_state['count'] reaches done_after."""

    def __init__(self, done_after=1):
        self.begin_count = 0
        self.consume_calls: list = []
        self.responses: list = []
        self.end_calls: list = []
        self.done_after = done_after

    async def begin_request(self, sc):
        self.begin_count += 1
        return {"count": 0}

    async def consume_message(self, sc, handler_state, value, sender, result_ctx):
        self.consume_calls.append(value)
        await sender.send(value)
        handler_state["count"] += 1
        if handler_state["count"] >= self.done_after:
            result_ctx.done()

    async def handle_response(self, sc, handler_state, response):
        self.responses.append(response)

    async def end_request(self, sc, err, handler_state):
        self.end_calls.append(err)


class _FailingBeginHandler:
    async def begin_request(self, sc):
        raise RuntimeError("begin boom")

    async def consume_message(self, sc, handler_state, value, sender, result_ctx):
        raise AssertionError("must not be called")

    async def handle_response(self, sc, handler_state, response):
        raise AssertionError("must not be called")

    async def end_request(self, sc, err, handler_state):
        raise AssertionError("must not be called")


# ---------------------------------------------------------------------------
# client-streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_client_streaming_single_message():
    call = _FakeClientStreamingCall(response="resp")
    handler = _RecordingHandler(done_after=1)
    endpoint = _FakeEndpoint()
    stream = _FakeStream()

    async def client_fn(metadata=()):
        return call

    consumer = _ClientStreamingSinkConsumer(
        endpoint, stream, handler, None, None, client_fn,
    )
    with_stream_id("s1")
    await consumer.consume("v1")
    await asyncio.sleep(0.05)  # let the detached _complete task run

    assert handler.begin_count == 1
    assert handler.consume_calls == ["v1"]
    assert call.writes == ["v1"]
    assert call.done_writing_called
    assert handler.responses == ["resp"]
    assert handler.end_calls == [None]
    assert endpoint.request_ends == [None]


@pytest.mark.asyncio
async def test_client_streaming_multiple_messages_reuse_one_stream():
    """The core Go-parity fix: N Consume() calls for the same stream_id must
    share ONE client_fn call / gRPC stream, not open N independent ones."""
    call = _FakeClientStreamingCall(response="resp")
    handler = _RecordingHandler(done_after=3)
    endpoint = _FakeEndpoint()
    stream = _FakeStream()
    call_count = 0

    async def client_fn(metadata=()):
        nonlocal call_count
        call_count += 1
        return call

    consumer = _ClientStreamingSinkConsumer(
        endpoint, stream, handler, None, None, client_fn,
    )
    with_stream_id("order-1")
    await consumer.consume("item1")
    await consumer.consume("item2")
    await consumer.consume("item3")
    await asyncio.sleep(0.05)

    assert call_count == 1, "client_fn must be called exactly once per stream_id"
    assert handler.begin_count == 1, "begin_request must be called exactly once per stream_id"
    assert handler.consume_calls == ["item1", "item2", "item3"]
    assert call.writes == ["item1", "item2", "item3"]
    assert handler.responses == ["resp"]  # handle_response called once, after the last message
    assert handler.end_calls == [None]


@pytest.mark.asyncio
async def test_client_streaming_different_stream_ids_independent():
    calls: dict[str, _FakeClientStreamingCall] = {}

    async def client_fn(metadata=()):
        call = _FakeClientStreamingCall(response="resp")
        return call

    handler_a = _RecordingHandler(done_after=1)
    handler_b = _RecordingHandler(done_after=1)
    endpoint = _FakeEndpoint()
    stream = _FakeStream()

    consumer_a = _ClientStreamingSinkConsumer(
        endpoint, stream, handler_a, None, None, client_fn,
    )
    with_stream_id("stream-a")
    await consumer_a.consume("a1")

    consumer_b = _ClientStreamingSinkConsumer(
        endpoint, stream, handler_b, None, None, client_fn,
    )
    with_stream_id("stream-b")
    await consumer_b.consume("b1")

    await asyncio.sleep(0.05)

    assert handler_a.consume_calls == ["a1"]
    assert handler_b.consume_calls == ["b1"]


@pytest.mark.asyncio
async def test_client_streaming_begin_request_failure():
    endpoint = _FakeEndpoint()
    stream = _FakeStream()
    handler = _FailingBeginHandler()

    async def client_fn(metadata=()):
        raise AssertionError("must not be called when begin_request fails")

    consumer = _ClientStreamingSinkConsumer(
        endpoint, stream, handler, None, None, client_fn,
    )
    with_stream_id("s-fail")
    await consumer.consume("v1")

    assert len(endpoint.begin_failures) == 1
    # A later Consume for the same stream_id must be able to retry from scratch.
    retried = False

    async def client_fn2(metadata=()):
        nonlocal retried
        retried = True
        return _FakeClientStreamingCall(response="ok")

    handler2 = _RecordingHandler(done_after=1)
    consumer2 = _ClientStreamingSinkConsumer(
        endpoint, stream, handler2, None, None, client_fn2,
    )
    with_stream_id("s-fail")
    await consumer2.consume("v2")
    await asyncio.sleep(0.05)
    assert retried
    assert handler2.consume_calls == ["v2"]


@pytest.mark.asyncio
async def test_client_streaming_grpc_call_failure_drops_reservation():
    endpoint = _FakeEndpoint()
    stream = _FakeStream()
    handler = _RecordingHandler(done_after=1)

    async def failing_client_fn(metadata=()):
        raise RuntimeError("dial failed")

    consumer = _ClientStreamingSinkConsumer(
        endpoint, stream, handler, None, None, failing_client_fn,
    )
    with_stream_id("s-dial-fail")
    await consumer.consume("v1")

    assert len(handler.end_calls) == 1
    assert isinstance(handler.end_calls[0], RuntimeError)
    assert str(handler.end_calls[0]) == "dial failed"
    # pending map must not retain the failed reservation.
    current, found = consumer._pending.get("s-dial-fail")
    assert not found


# ---------------------------------------------------------------------------
# bidi-streaming
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bidi_streaming_multiple_messages_reuse_one_stream():
    call = _FakeBidiStreamingCall(responses=["r1", "r2"])
    handler = _RecordingHandler(done_after=2)
    endpoint = _FakeEndpoint()
    stream = _FakeStream()
    call_count = 0

    async def client_fn(metadata=()):
        nonlocal call_count
        call_count += 1
        return call

    consumer = _BidiStreamingSinkConsumer(
        endpoint, stream, handler, None, None, client_fn,
    )
    with_stream_id("bidi-1")
    await consumer.consume("m1")
    await consumer.consume("m2")
    await asyncio.sleep(0.05)

    assert call_count == 1, "client_fn must be called exactly once per stream_id"
    assert handler.begin_count == 1
    assert handler.consume_calls == ["m1", "m2"]
    assert call.writes == ["m1", "m2"]
    assert call.done_writing_called
    assert handler.responses == ["r1", "r2"]
    assert handler.end_calls == [None]

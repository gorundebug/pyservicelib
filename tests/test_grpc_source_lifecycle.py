import asyncio
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest

from pyservicelib_gorundebug.datasource.grpc.grpcds import (
    _GrpcEndpoint, _GrpcTypedEndpointConsumer, _StreamSender, _UnarySender,
)
from pyservicelib_gorundebug.runtime.common import TypedInputStream
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import request_stream_id

from .test_grpc_sink_streaming import _FakeEndpoint, _FakeStream

MODES = ("unary", "server", "client", "bidi")


class Endpoint(_FakeEndpoint):
    def __init__(self):
        super().__init__()
        self.pending = 0

    def on_pending_add(self, stream_id):
        self.pending += 1

    def on_pending_remove(self, stream_id):
        self.pending -= 1

    def on_missing_stream_id(self):
        raise AssertionError("stream ID disappeared")

    def on_unknown_message_id(self, stream_id, message_id):
        pass

    def on_duplicate_message_id(self, stream_id, message_id):
        pass


class Input(_FakeStream):
    result_consumer: Any = None

    def __init__(self, has_result):
        super().__init__()
        self.has_result = has_result

    def get_result_stream(self):
        return self if self.has_result else None

    def set_result_consumer(self, consumer):
        self.result_consumer = consumer

    async def consume(self, value):
        pass


class Handler:
    def __init__(self):
        self.consumed = []
        self.ended = []
        self.eofs = 0

    async def begin_request(self, sc):
        return object()

    async def consume_message(self, sc, handler_state, req, result_ctx, sender):
        self.consumed.append(req)
        await sender.send(req)
        result_ctx.done()

    def get_message_id(self, sc, handler_state, value):
        return value

    def eof(self, sc, handler_state):
        self.eofs += 1

    async def end_request(self, sc, err, handler_state):
        self.ended.append(err)


@asynccontextmanager
async def source(handler, has_result):
    endpoint = Endpoint()
    consumer = _GrpcTypedEndpointConsumer(
        cast(_GrpcEndpoint, endpoint),
        cast(TypedInputStream[Any, Any, Any], Input(has_result)), handler,
    )
    token = request_stream_id.set(None)
    await consumer.start(Context())
    try:
        yield consumer, endpoint
    finally:
        await consumer.stop(Context())
        request_stream_id.reset(token)


async def invoke(consumer, mode, value="message", request_iter=None):
    async def write(response):
        pass

    async def messages():
        yield value

    sender: _UnarySender[Any] | _StreamSender[Any] = (
        _UnarySender() if mode in ("unary", "client") else _StreamSender(write)
    )
    return await consumer._handle_common_inner(
        "source-session", value, sender, mode in ("unary", "server"),
        (request_iter if request_iter is not None else messages())
        if mode in ("client", "bidi") else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("has_result", [False, True])
async def test_source_reserves_stream_id_through_end_request(mode, has_result):
    entered = asyncio.Event()
    release = asyncio.Event()

    class Finalizing(Handler):
        async def end_request(self, sc, err, handler_state):
            await super().end_request(sc, err, handler_state)
            if err is None:
                entered.set()
                await release.wait()

    handler = Finalizing()
    async with source(handler, has_result) as (consumer, endpoint):
        first = asyncio.create_task(invoke(consumer, mode, "first"))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            error = await asyncio.wait_for(invoke(consumer, mode, "second"), 1)
            assert error is not None
            assert handler.consumed == ["first"]
            assert len(endpoint.begin_failures) == 1
        finally:
            release.set()
            await asyncio.wait_for(first, 1)
        assert endpoint.pending == 0
        assert await asyncio.wait_for(invoke(consumer, mode, "reused"), 1) is None
        assert handler.consumed == ["first", "reused"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("has_result", [False, True])
async def test_source_consume_failure_skips_eof_and_end_can_handle_error(mode, has_result):
    failure = ValueError("business handler failed")

    class Failing(Handler):
        async def consume_message(self, sc, handler_state, req, result_ctx, sender):
            raise failure

    handler = Failing()
    async with source(handler, has_result) as (consumer, endpoint):
        result = await asyncio.wait_for(invoke(consumer, mode), 1)
        assert handler.ended == [failure]
        assert handler.eofs == 0
        assert result is None
        assert endpoint.pending == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["client", "bidi"])
@pytest.mark.parametrize("has_result", [False, True])
async def test_source_receive_failure_finalizes_and_releases_reservation(mode, has_result):
    failure = ValueError("receive failed")

    async def broken():
        yield "first"
        raise failure

    handler = Handler()
    async with source(handler, has_result) as (consumer, endpoint):
        result = await asyncio.wait_for(invoke(consumer, mode, request_iter=broken()), 1)
        assert handler.ended == [failure]
        assert handler.eofs == 0
        assert result is None
        assert endpoint.pending == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_source_end_waits_for_active_result_callback(mode):
    callback_entered = asyncio.Event()
    release = asyncio.Event()
    ended = asyncio.Event()
    callback_task = None

    class DelayedCallback(Handler):
        async def consume_message(self, sc, handler_state, req, result_ctx, sender):
            nonlocal callback_task

            async def callback(sc, handler_state, value, sender):
                await sender.send(value)
                result_ctx.done()
                callback_entered.set()
                await release.wait()
                return True

            result_ctx.set_result_callback(req, callback)
            callback_task = asyncio.create_task(consumer._consume_result(req))

        async def end_request(self, sc, err, handler_state):
            await super().end_request(sc, err, handler_state)
            ended.set()

    handler = DelayedCallback()
    async with source(handler, True) as (consumer, endpoint):
        task = asyncio.create_task(invoke(consumer, mode))
        try:
            await asyncio.wait_for(callback_entered.wait(), 1)
            for _ in range(5):
                await asyncio.sleep(0)
            assert not ended.is_set()
            assert endpoint.pending == 1
        finally:
            release.set()
            if callback_task is not None:
                await asyncio.wait_for(callback_task, 1)
            await asyncio.wait_for(task, 1)
        assert handler.ended == [None]
        assert endpoint.pending == 0

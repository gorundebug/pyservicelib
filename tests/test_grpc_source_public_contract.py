import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from pyservicelib_gorundebug.datasource.grpc import grpcds
from pyservicelib_gorundebug.runtime.context import Context

from .test_grpc_source_lifecycle import Endpoint, Handler, Input


class PublicInput(Input):
    endpoint_id = 1
    environment = SimpleNamespace(tracing=None)


class RpcContext:
    def __init__(self) -> None:
        self.done_callbacks: list[Any] = []

    def time_remaining(self) -> None:
        return None

    def add_done_callback(self, callback: Any) -> None:
        self.done_callbacks.append(callback)

    def cancelled(self) -> bool:
        return False

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("x-stream-id", "public-source-session"),)

    async def abort(self, code: Any, details: str) -> None:
        raise AssertionError(f"unexpected RPC abort: {code}: {details}")

    async def write(self, value: Any) -> None:
        pass


@asynccontextmanager
async def public_source(
    monkeypatch: pytest.MonkeyPatch, mode: str, handler: Any, has_result: bool,
) -> AsyncIterator[tuple[Any, Endpoint]]:
    endpoint = Endpoint()
    monkeypatch.setattr(grpcds, "_get_or_create_datasource", lambda *args: object())
    monkeypatch.setattr(grpcds, "_get_or_create_endpoint", lambda *args: endpoint)
    factory = getattr(grpcds, f"make_grpc_{mode}_endpoint_consumer")
    consumer, handle = factory(PublicInput(has_result), handler)
    await consumer.start(Context())
    try:
        yield handle, endpoint
    finally:
        await consumer.stop(Context())


async def call(handle: Any, mode: str) -> Any:
    async def requests() -> AsyncIterator[str]:
        yield "request"

    request: Any = requests() if mode in ("client_streaming", "bidi_streaming") else "request"
    return await handle(request, RpcContext())


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["no_streaming", "server_streaming", "client_streaming", "bidi_streaming"])
async def test_public_source_without_result_finishes_without_send(
    monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    class NoReply(Handler):
        async def consume_message(self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any) -> None:
            self.consumed.append(req)

    handler = NoReply()
    async with public_source(monkeypatch, mode, handler, False) as (handle, endpoint):
        assert await asyncio.wait_for(call(handle, mode), 1) is None
        assert handler.consumed == ["request"]
        assert handler.eofs == 1
        assert handler.ended == [None]
        assert endpoint.pending == 0


@pytest.mark.asyncio
async def test_public_client_done_allows_end_request_to_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FinishInEnd(Handler):
        sender: Any = None

        async def consume_message(self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any) -> None:
            self.sender = sender
            result_ctx.done()

        async def end_request(self, sc: Any, err: Any, handler_state: Any) -> None:
            self.ended.append(err)
            await self.sender.send("final response")

    handler = FinishInEnd()
    async with public_source(monkeypatch, "client_streaming", handler, True) as (handle, endpoint):
        task = asyncio.create_task(call(handle, "client_streaming"))
        try:
            assert await asyncio.wait_for(asyncio.shield(task), 1) == "final response"
            assert handler.ended == [None]
            assert endpoint.pending == 0
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_public_client_sender_accepts_only_first_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SendTwice(Handler):
        async def consume_message(self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any) -> None:
            await sender.send("first")
            await sender.send("second")

    handler = SendTwice()
    async with public_source(monkeypatch, "client_streaming", handler, True) as (handle, endpoint):
        assert await asyncio.wait_for(call(handle, "client_streaming"), 1) == "first"
        assert handler.ended == [None]
        assert handler.eofs == 1
        assert endpoint.pending == 0


@pytest.mark.asyncio
async def test_public_unary_done_does_not_replace_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = asyncio.Event()

    class FinishLater(Handler):
        sender: Any = None

        async def consume_message(self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any) -> None:
            self.sender = sender
            result_ctx.done()
            ready.set()

    handler = FinishLater()
    async with public_source(monkeypatch, "no_streaming", handler, True) as (handle, endpoint):
        task = asyncio.create_task(call(handle, "no_streaming"))
        try:
            await asyncio.wait_for(ready.wait(), 1)
            assert not task.done()
            assert handler.ended == []
            await handler.sender.send("actual response")
            assert await asyncio.wait_for(task, 1) == "actual response"
            assert handler.ended == [None]
            assert endpoint.pending == 0
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

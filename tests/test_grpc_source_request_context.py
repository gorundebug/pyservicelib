import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import grpc
import grpc.aio
import pytest

from pyservicelib_gorundebug.datasource.grpc.grpcds import _StreamSender, _UnarySender
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled, request_deadline, request_stream_id,
)

from .test_grpc_source_lifecycle import Handler, source


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["unary_unary", "unary_stream", "stream_unary", "stream_stream"])
@pytest.mark.parametrize("check", ["deadline", "cancellation"])
async def test_grpc_transport_context_reaches_pipeline(mode: str, check: str) -> None:
    entered = asyncio.Event()
    ended = asyncio.Event()
    handled = asyncio.Event()
    release = asyncio.Event()
    observed: dict[str, Any] = {}

    class ObserveContext(Handler):
        async def consume_message(
            self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any,
        ) -> None:
            observed["deadline"] = request_deadline.get()
            observed["cancelled"] = request_cancelled.get()
            observed["stream_id"] = request_stream_id.get()
            entered.set()
            await release.wait()
            await sender.send(b"response")
            result_ctx.done()

        async def end_request(self, sc: Any, err: Any, handler_state: Any) -> None:
            observed["end_error"] = err
            ended.set()

    handler = ObserveContext()
    parent_deadline = request_deadline.get()
    parent_cancelled = request_cancelled.get()
    parent_id = request_stream_id.get()
    async with source(handler, True) as (consumer, endpoint):
        async def handle(request: Any, context: Any) -> Any:
            sender: _UnarySender[bytes] | _StreamSender[bytes]
            sender = _UnarySender() if mode.endswith("unary") else _StreamSender(context.write)
            # Exercise the shared transport-context boundary independently of
            # the public factories' separate response-completion contract.
            try:
                await consumer._handle_common(
                    context, {}, "network-context-session",
                    None if mode.startswith("stream") else request, sender,
                    mode.startswith("unary"),
                    request if mode.startswith("stream") else None,
                )
                if isinstance(sender, _UnarySender) and sender._future.done():
                    return sender._future.result()
                return None
            finally:
                handled.set()

        server = grpc.aio.server()
        method_factory = getattr(grpc, f"{mode}_rpc_method_handler")
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "ContextProbe", {"Call": method_factory(handle)},
        ),))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        call: Any = None

        async def requests() -> AsyncIterator[bytes]:
            yield b"request"

        try:
            before = datetime.now(timezone.utc)
            method = getattr(channel, mode)("/ContextProbe/Call")
            call = method(requests() if mode.startswith("stream") else b"request", timeout=10)
            await asyncio.wait_for(entered.wait(), 3)
            assert observed["stream_id"] == "network-context-session"
            if check == "deadline":
                deadline = observed["deadline"]
                assert deadline is not None, "RPC deadline was not exposed to the pipeline"
                assert 0 < (deadline - before).total_seconds() <= 11
            else:
                call.cancel()
                await asyncio.wait_for(ended.wait(), 3)
                cancelled = observed["cancelled"]
                assert cancelled is not None, "RPC cancellation event was not exposed to the pipeline"
                assert cancelled.is_set(), "transport cancellation did not signal the pipeline"
                assert observed["end_error"] is not None
        finally:
            if call is not None:
                call.cancel()
            release.set()
            try:
                if entered.is_set():
                    await asyncio.wait_for(handled.wait(), 3)
            finally:
                await channel.close(grace=None)
                await server.stop(None)
        assert endpoint.pending == 0
    assert request_deadline.get() is parent_deadline
    assert request_cancelled.get() is parent_cancelled
    assert request_stream_id.get() == parent_id

import asyncio
from datetime import datetime, timezone
from typing import Any, cast

import grpc
import grpc.aio
import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import (
    _GrpcSinkEndpoint, _NoStreamingSinkConsumer,
)
from pyservicelib_gorundebug.runtime.common import TypedSinkStreamWithResult
from pyservicelib_gorundebug.runtime.context.request import request_deadline

from .test_grpc_sink_streaming import _FakeEndpoint, _FakeStream, _RecordingHandler
from .test_grpc_source_lifecycle import Handler
from .test_grpc_source_public_contract import public_source


@pytest.mark.asyncio
@pytest.mark.parametrize("child_task", [False, True])
async def test_two_grpc_services_forward_only_remaining_budget(
    monkeypatch: pytest.MonkeyPatch, child_task: bool,
) -> None:
    observed: dict[str, Any] = {}
    downstream_channel: Any = None

    class Downstream(Handler):
        async def consume_message(
            self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any,
        ) -> None:
            observed["downstream_deadline"] = request_deadline.get()
            await sender.send(req)

    class Upstream(Handler):
        async def consume_message(
            self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any,
        ) -> None:
            observed["upstream_deadline"] = request_deadline.get()
            observed["entered_at"] = datetime.now(timezone.utc)

            async def forward() -> None:
                await asyncio.sleep(0.15)
                handler = _RecordingHandler()

                async def client(req: Any, metadata: Any = (), timeout: Any = None) -> Any:
                    observed["forwarded_timeout"] = timeout
                    observed["sent_at"] = datetime.now(timezone.utc)
                    return await downstream_channel.unary_unary("/Budget/Call")(
                        req, metadata=metadata, timeout=timeout,
                    )

                consumer = _NoStreamingSinkConsumer(
                    cast(_GrpcSinkEndpoint, _FakeEndpoint()),
                    cast(TypedSinkStreamWithResult[Any, Any, Any], _FakeStream()),
                    handler, None, None, client,
                )
                await consumer.consume(req)
                observed["sink_end_errors"] = handler.end_calls
                if handler.responses:
                    await sender.send(handler.responses[0])

            if child_task:
                await asyncio.create_task(forward())
            else:
                await forward()

    async with public_source(monkeypatch, "no_streaming", Downstream(), False) as (down_handle, _):
        async with public_source(monkeypatch, "no_streaming", Upstream(), False) as (up_handle, _):
            servers = [grpc.aio.server(), grpc.aio.server()]
            ports: list[int] = []
            for server, handle in zip(servers, (down_handle, up_handle)):
                server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
                    "Budget", {"Call": grpc.unary_unary_rpc_method_handler(handle)},
                ),))
                ports.append(server.add_insecure_port("127.0.0.1:0"))
                await server.start()
            downstream_channel = grpc.aio.insecure_channel(f"127.0.0.1:{ports[0]}")
            upstream_channel = grpc.aio.insecure_channel(f"127.0.0.1:{ports[1]}")
            try:
                response = await upstream_channel.unary_unary(
                    "/Budget/Call", request_serializer=None, response_deserializer=None,
                )(b"request", timeout=3)
                assert response == b"request"
                assert observed["sink_end_errors"] == [None]
                first = observed["upstream_deadline"]
                second = observed["downstream_deadline"]
                sent_at = observed["sent_at"]
                assert first is not None and second is not None
                initial_budget = (first - observed["entered_at"]).total_seconds()
                timeout = observed["forwarded_timeout"]
                assert 0 < timeout <= initial_budget - 0.12
                assert abs(timeout - (first - sent_at).total_seconds()) < 0.05
                # grpc-timeout has finite wire precision, so allow a small
                # rounding/transit margin, but never the spent 150 ms again.
                assert abs((second - first).total_seconds()) < 0.05
            finally:
                await upstream_channel.close(grace=None)
                await downstream_channel.close(grace=None)
                for server in reversed(servers):
                    await server.stop(None)

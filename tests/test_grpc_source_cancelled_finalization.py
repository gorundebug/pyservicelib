import asyncio
from typing import Any

import pytest

from .test_grpc_source_lifecycle import Handler, MODES, invoke, source


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("phase", ["callback", "end"])
@pytest.mark.parametrize("cancel_count", [1, 2])
async def test_source_cancellation_retains_active_callback_and_end_request(
    mode: str, phase: str, cancel_count: int,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    callbacks: list[asyncio.Task[Any]] = []
    consumer: Any = None

    class Paused(Handler):
        async def consume_message(
            self, sc: Any, handler_state: Any, req: Any, result_ctx: Any, sender: Any,
        ) -> None:
            if phase == "end":
                await super().consume_message(sc, handler_state, req, result_ctx, sender)
                return

            async def callback(sc: Any, state: Any, value: Any, output: Any) -> bool:
                await output.send(value)
                result_ctx.done()
                entered.set()
                await release.wait()
                return True

            result_ctx.set_result_callback(req, callback)
            callbacks.append(asyncio.create_task(consumer._consume_result(req)))

        async def end_request(self, sc: Any, err: Any, handler_state: Any) -> None:
            self.ended.append(err)
            if phase == "end":
                entered.set()
                await release.wait()

    handler = Paused()
    async with source(handler, True) as (core, endpoint):
        consumer = core
        request = asyncio.create_task(invoke(core, mode))
        try:
            await asyncio.wait_for(entered.wait(), 2)

            async def wait_for_terminal_handling() -> None:
                assert core._pending is not None
                while True:
                    result, found = core._pending.get("source-session")
                    assert found
                    assert result is not None
                    if result.closed:
                        return
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_terminal_handling(), 2)
            for _ in range(cancel_count):
                request.cancel()
                await asyncio.sleep(0)
            assert endpoint.pending == 1, "cancellation released a still-active request"
            assert not request.done(), "cancellation bypassed active finalization"
            if phase == "callback":
                assert handler.ended == []
        finally:
            release.set()
            if callbacks:
                await asyncio.wait_for(asyncio.gather(*callbacks), 2)
            results = await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), 2)
        assert results == [None]
        assert handler.ended == [None]
        assert endpoint.pending == 0

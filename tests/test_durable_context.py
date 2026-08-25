import asyncio
from datetime import timedelta

import pytest

from pyservicelib_gorundebug.runtime.durable_context import (
    DurableCallAlreadyCompletedError,
    DurableCallContext,
    DurableCallHeartbeatAfterCompletionError,
    DurableCallOutcomeMissingError,
    NoDurableCallContextError,
    bind_durable_call_span,
    begin_durable_delay,
    capture_durable_continuation,
    durable_call_error,
    durable_call_heartbeat,
    durable_call_success,
    run_durable_call_activity,
)
from pyservicelib_gorundebug.runtime.environment.tracing import (
    Attribute,
    Span,
    SpanContext,
    StatusCode,
)


@pytest.mark.asyncio
async def test_explicit_success_is_required_without_deadline() -> None:
    durable = DurableCallContext("parent")
    entered = asyncio.Event()
    complete = asyncio.Event()

    async def invoke() -> None:
        async def finish() -> None:
            await complete.wait()
            durable_call_success()

        asyncio.create_task(finish())
        entered.set()

    task = asyncio.create_task(run_durable_call_activity(durable, invoke))
    await entered.wait()
    await asyncio.sleep(0)
    assert not task.done()
    complete.set()
    await task


async def _complete_from_child_context() -> None:
    durable_call_success()


def test_operation_outside_activity_is_observable() -> None:
    with pytest.raises(NoDurableCallContextError):
        durable_call_success()


@pytest.mark.asyncio
async def test_activity_context_propagates_to_graph_task() -> None:
    durable = DurableCallContext("parent")

    async def invoke() -> None:
        await asyncio.create_task(_complete_from_child_context())

    await run_durable_call_activity(durable, invoke)


@pytest.mark.asyncio
async def test_first_terminal_wins_and_late_heartbeat_is_rejected() -> None:
    durable = DurableCallContext("parent")

    async def invoke() -> None:
        durable_call_heartbeat("half-way")
        durable_call_success()
        with pytest.raises(DurableCallAlreadyCompletedError):
            durable_call_error(RuntimeError("too late"))
        with pytest.raises(DurableCallHeartbeatAfterCompletionError):
            durable_call_heartbeat("too late")

    await run_durable_call_activity(durable, invoke)


@pytest.mark.asyncio
async def test_cancellation_supplies_missing_outcome() -> None:
    durable = DurableCallContext("parent")
    started = asyncio.Event()

    async def invoke() -> None:
        started.set()
        await asyncio.Future()

    task = asyncio.create_task(run_durable_call_activity(durable, invoke))
    await started.wait()
    task.cancel()
    with pytest.raises(DurableCallOutcomeMissingError):
        await task


@pytest.mark.asyncio
async def test_lifecycle_events_are_attached_to_activity_span() -> None:
    class RecordedSpan(Span):
        def __init__(self) -> None:
            self.events: list[str] = []
            self.status_code = StatusCode.UNSET
            self.ended = False

        def end(self) -> None:
            self.ended = True

        def set_attributes(self, *attrs: Attribute) -> None:
            del attrs

        def record_error(self, err: Exception) -> None:
            del err

        def set_status(self, code: int, description: str) -> None:
            del description
            self.status_code = code

        def add_event(self, name: str, *attrs: Attribute) -> None:
            del attrs
            self.events.append(name)

        def span_context(self) -> SpanContext:
            return SpanContext()

    durable = DurableCallContext("parent")
    span = RecordedSpan()

    async def invoke() -> None:
        assert bind_durable_call_span(span)
        durable_call_heartbeat("half-way")
        durable_call_success()

    await run_durable_call_activity(durable, invoke)
    assert span.status_code == StatusCode.OK
    assert span.ended
    assert span.events == [
        "durable_call.heartbeat",
        "durable_call.success",
    ]


@pytest.mark.asyncio
async def test_durable_delay_returns_serializable_continuation() -> None:
    durable = DurableCallContext("call-1")

    async def invoke() -> None:
        assert begin_durable_delay(timedelta(hours=1))
        assert capture_durable_continuation(
            "Delay", "After Delay", b"value"
        )

    result = await run_durable_call_activity(durable, invoke)
    assert result.continuation is not None
    assert result.continuation.from_name == "Delay"
    assert result.continuation.to_name == "After Delay"
    assert result.continuation.call_id == "call-1/delay"
    assert result.continuation.payload == b"value"


def test_begin_durable_delay_keeps_ordinary_context_local() -> None:
    assert not begin_durable_delay(timedelta(hours=1))

import asyncio

import pytest

from pyservicelib_gorundebug.runtime.durable_context import (
    DurableCallContext,
    DurableCallHeartbeatAfterCompletionError,
    bind_durable_call_span,
    current_durable_call_context,
    durable_call_heartbeat,
    run_durable_call_activity,
)
from pyservicelib_gorundebug.runtime.environment.tracing import (
    Attribute,
    Span,
    SpanContext,
    StatusCode,
)


def test_heartbeat_outside_temporal_is_a_silent_noop() -> None:
    durable_call_heartbeat("ignored")


@pytest.mark.asyncio
async def test_activity_context_propagates_to_graph_task_and_returns_result() -> None:
    durable = DurableCallContext("message-1")

    async def invoke() -> str:
        async def inspect() -> None:
            assert current_durable_call_context() is durable

        await asyncio.create_task(inspect())
        return "done"

    assert await run_durable_call_activity(durable, invoke) == "done"
    assert current_durable_call_context() is None


@pytest.mark.asyncio
async def test_activity_records_heartbeat_and_automatic_success() -> None:
    recorded: list[object] = []
    diagnostics: list[tuple[str, BaseException | None]] = []
    durable = DurableCallContext(
        "message-1",
        heartbeat=recorded.append,
        diagnostics=lambda event, error: diagnostics.append((event, error)),
    )

    async def invoke() -> None:
        durable_call_heartbeat({"step": 2})

    await run_durable_call_activity(durable, invoke)

    assert recorded == [{"step": 2}]
    assert diagnostics == [("heartbeat", None), ("success", None)]


@pytest.mark.asyncio
async def test_activity_records_automatic_error_and_propagates_it() -> None:
    diagnostics: list[tuple[str, BaseException | None]] = []
    durable = DurableCallContext(
        "message-1",
        diagnostics=lambda event, error: diagnostics.append((event, error)),
    )
    failure = RuntimeError("business failure")

    async def invoke() -> None:
        raise failure

    with pytest.raises(RuntimeError, match="business failure"):
        await run_durable_call_activity(durable, invoke)

    assert diagnostics == [("error", failure)]


@pytest.mark.asyncio
async def test_late_heartbeat_after_activity_completion_is_rejected() -> None:
    durable = DurableCallContext("message-1")
    captured: DurableCallContext | None = None

    async def invoke() -> None:
        nonlocal captured
        captured = current_durable_call_context()

    await run_durable_call_activity(durable, invoke)
    assert captured is durable
    with pytest.raises(DurableCallHeartbeatAfterCompletionError):
        captured.heartbeat("too late")


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

    durable = DurableCallContext("message-1")
    span = RecordedSpan()

    async def invoke() -> None:
        assert bind_durable_call_span(span)
        durable_call_heartbeat("half-way")

    await run_durable_call_activity(durable, invoke)
    assert span.status_code == StatusCode.OK
    assert span.ended
    assert span.events == [
        "temporal.activity.heartbeat",
        "temporal.activity.success",
    ]

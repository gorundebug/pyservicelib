import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from pyservicelib_gorundebug.datasource.temporal import workflow_context as module


@pytest.fixture
def workflow_clock(monkeypatch):
    clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    monkeypatch.setattr(module.workflow, "info", lambda: object())
    monkeypatch.setattr(module.workflow, "now", lambda: clock[0])
    monkeypatch.setattr(module, "current_workflow_carrier", lambda: {})
    return clock


@pytest.mark.asyncio
async def test_workflow_deadline_uses_temporal_clock_and_cannot_be_extended(workflow_clock, monkeypatch):
    envelope_deadline = int((workflow_clock[0] + timedelta(seconds=10)).timestamp() * 1_000_000_000)
    carrier_deadline = int((workflow_clock[0] + timedelta(seconds=5)).timestamp() * 1_000_000_000)
    monkeypatch.setattr(module, "current_workflow_carrier", lambda: {
        module.TEMPORAL_HEADER_DEADLINE_UNIX_NANO: str(carrier_deadline),
    })
    context = module.workflow_graph_context(envelope_deadline)
    child = context.child()
    extended = context.bounded(timedelta(seconds=30))
    shorter = context.bounded(timedelta(seconds=2))
    assert context.time_left == child.time_left == extended.time_left == 5
    assert shorter.time_left == 2
    workflow_clock[0] += timedelta(seconds=3)
    assert shorter.is_expired
    assert context.time_left == 2
    assert not child.is_expired
    workflow_clock[0] += timedelta(seconds=2)
    assert all(item.is_expired for item in (context, child, extended, shorter))


@pytest.mark.asyncio
async def test_workflow_children_preserve_owner_without_cancelling_siblings(workflow_clock):
    context = module.workflow_graph_context()
    child = context.child()
    sibling = context.child()
    assert context.time_left is None
    child.cancel()
    assert child.cancelled
    assert not context.cancelled
    assert not sibling.cancelled
    context.cancel()
    assert sibling.cancelled


@pytest.mark.asyncio
async def test_workflow_task_cancellation_reaches_existing_contexts(workflow_clock):
    ready = asyncio.Event()
    contexts = []

    async def run():
        context = module.workflow_graph_context()
        contexts.extend((context, context.child(), context.bounded(timedelta(seconds=10))))
        ready.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await ready.wait()
    assert all(not item.cancelled for item in contexts)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(item.cancelled and item.is_expired for item in contexts)


def test_workflow_context_rejects_process_code():
    with pytest.raises(Exception, match="[Ww]orkflow|[Ee]vent loop|[Rr]unning"):
        module.workflow_graph_context()

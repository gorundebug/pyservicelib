from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.datasource.temporal.workflow_environment import (
    TemporalWorkflowEnvironment,
)
from pyservicelib_gorundebug.runtime.context import Context


@pytest.mark.asyncio
async def test_workflow_finish_preserves_caller_context() -> None:
    environment = object.__new__(TemporalWorkflowEnvironment)
    environment._started = True
    environment._wait_for_quiescence = AsyncMock()
    pool = SimpleNamespace(stop=AsyncMock())
    priority_pool = SimpleNamespace(stop=AsyncMock())
    storage = SimpleNamespace(stop=AsyncMock())
    environment._task_pools = {"pool": pool}
    environment._priority_task_pools = {"priority": priority_pool}
    environment._storages = [storage]
    environment._tasks = set()
    environment._metrics = None
    environment._failure = None
    ctx = Context()

    await environment.finish(ctx)

    for resource in (pool, priority_pool, storage):
        resource.stop.assert_awaited_once_with(ctx)
    assert environment._started is False


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["stop", "release"])
async def test_workflow_cleanup_forwards_context(operation: str) -> None:
    environment = object.__new__(TemporalWorkflowEnvironment)
    environment.finish = AsyncMock()
    ctx = Context()

    await getattr(environment, operation)(ctx)

    environment.finish.assert_awaited_once_with(ctx)


@pytest.mark.asyncio
async def test_workflow_start_preserves_caller_context(monkeypatch) -> None:
    from pyservicelib_gorundebug.datasource.temporal import workflow_environment

    monkeypatch.setattr(workflow_environment.workflow, "info", lambda: SimpleNamespace(
        workflow_id="caller-workflow", workflow_type="context-test",
    ))
    environment = object.__new__(TemporalWorkflowEnvironment)
    environment._started = False
    environment._log = SimpleNamespace(info=lambda *args: None)
    environment._streams = {}
    pool = SimpleNamespace(start=AsyncMock())
    priority_pool = SimpleNamespace(start=AsyncMock())
    storage = SimpleNamespace(start=AsyncMock())
    environment._task_pools = {"pool": pool}
    environment._priority_task_pools = {"priority": priority_pool}
    environment._storages = [storage]
    context = Context()
    await environment.start(context)
    for resource in (pool, priority_pool, storage):
        resource.start.assert_awaited_once_with(context)
    assert environment._started is True

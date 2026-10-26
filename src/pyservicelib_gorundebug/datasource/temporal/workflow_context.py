"""Lifecycle context backed by the current Temporal Workflow execution."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from temporalio import workflow

from ...runtime.context import Context
from .context_propagation import (
    TEMPORAL_HEADER_DEADLINE_UNIX_NANO,
    current_workflow_carrier,
)


def workflow_graph_context(deadline_unix_nano: int = 0) -> Context:
    """Preserve Workflow cancellation and the earliest carried/envelope deadline.

    This adapter is only for Workflow-isolate code, never Activity or process I/O.
    It adds no timers or tasks and uses Temporal's replay-safe clock.
    """
    workflow.info()  # Reject use outside a Workflow execution.
    owner = asyncio.current_task()
    if owner is None:
        raise RuntimeError("Temporal Workflow task is not available")
    deadlines = [deadline_unix_nano] if deadline_unix_nano > 0 else []
    carried = current_workflow_carrier().get(TEMPORAL_HEADER_DEADLINE_UNIX_NANO)
    if carried is not None:
        value = int(carried)
        if value > 0:
            deadlines.append(value)
    deadline = (
        datetime.fromtimestamp(min(deadlines) / 1_000_000_000, timezone.utc)
        if deadlines else None
    )
    return _WorkflowContext(owner, deadline)


class _WorkflowContext(Context):
    def __init__(
        self,
        owner: asyncio.Task[Any],
        deadline: datetime | None,
        parent: Context | None = None,
    ) -> None:
        super().__init__(_parent=parent)
        self._owner = owner
        self._workflow_deadline = deadline

    @property
    def cancelled(self) -> bool:
        return super().cancelled or self._owner.cancelling() > 0

    @property
    def time_left(self) -> float | None:
        if self._workflow_deadline is None:
            return None
        return max(0.0, (self._workflow_deadline - workflow.now()).total_seconds())

    @property
    def is_expired(self) -> bool:
        remaining = self.time_left
        return self.cancelled or (remaining is not None and remaining <= 0)

    def child(self) -> Context:
        return _WorkflowContext(self._owner, self._workflow_deadline, self)

    def bounded(self, timeout: timedelta) -> Context:
        deadline = workflow.now() + max(timeout, timedelta())
        if self._workflow_deadline is not None:
            deadline = min(deadline, self._workflow_deadline)
        return _WorkflowContext(self._owner, deadline, self)

"""Explicit, deterministic per-call Temporal scheduling overrides."""

from dataclasses import dataclass
from datetime import timedelta

from ...api.models.temporal_execution_type import TemporalExecutionType


@dataclass(frozen=True, slots=True)
class SubmissionOptions:
    # Empty retains the configured queue; this never implements failover.
    task_queue: str = ""
    schedule_to_start_timeout: timedelta = timedelta(0)

    def resolve(self, queue: str, execution: TemporalExecutionType) -> str:
        if self.task_queue and (not self.task_queue.strip() or "\0" in self.task_queue):
            raise ValueError("Temporal submission task queue is invalid")
        if self.schedule_to_start_timeout < timedelta(0):
            raise ValueError("Temporal submission schedule-to-start timeout must not be negative")
        if self.schedule_to_start_timeout and execution is not TemporalExecutionType.ACTIVITY:
            raise ValueError("Temporal submission schedule-to-start timeout requires an Activity endpoint")
        return self.task_queue or queue

#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from typing import Protocol

from .common import Collect


class ScheduleBackend(StrEnum):
    LOCAL = "local"
    TEMPORAL = "temporal"


@dataclass(frozen=True, slots=True)
class ScheduleTrigger:
    """Typed trigger shared by local Cron and Temporal Schedule endpoints."""

    trigger_id: str
    schedule_id: str
    scheduled_at: datetime
    fired_at: datetime
    backend: ScheduleBackend


class ScheduleEndpointFunction[T](Protocol):
    """User conversion boundary shared by local and Temporal schedules."""

    async def on_trigger(
        self, trigger: ScheduleTrigger, out: Collect[T]
    ) -> None: ...


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _rfc3339_nano(value: datetime) -> str:
    value = _utc(value)
    fraction = f"{value.microsecond:06d}".rstrip("0")
    return value.strftime("%Y-%m-%dT%H:%M:%S") + (
        f".{fraction}" if fraction else ""
    ) + "Z"


def new_schedule_trigger(
    endpoint_id: int,
    schedule_id: str,
    scheduled_at: datetime,
    fired_at: datetime,
    backend: ScheduleBackend,
) -> ScheduleTrigger:
    scheduled_at = _utc(scheduled_at)
    fired_at = _utc(fired_at)
    identity = (
        "servicegen:schedule-trigger:v1\n"
        f"{endpoint_id}\n{schedule_id}\n{_rfc3339_nano(scheduled_at)}"
    )
    return ScheduleTrigger(
        trigger_id=sha256(identity.encode()).hexdigest(),
        schedule_id=schedule_id,
        scheduled_at=scheduled_at,
        fired_at=fired_at,
        backend=backend,
    )


def normalize_temporal_priority(priority: int) -> int:
    if priority <= -2:
        return 1
    if priority == -1:
        return 2
    if priority == 0:
        return 3
    if priority == 1:
        return 4
    return 5

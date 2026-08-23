from datetime import datetime, timedelta, timezone

from pyservicelib_gorundebug.runtime.schedule import (
    ScheduleBackend,
    new_schedule_trigger,
    normalize_temporal_priority,
)


def test_schedule_trigger_identity_is_stable_across_retry_and_timezone() -> None:
    scheduled = datetime(2026, 8, 24, 12, 30, 0, 123456, tzinfo=timezone.utc)
    first = new_schedule_trigger(
        17, "hourly", scheduled, scheduled, ScheduleBackend.TEMPORAL
    )
    retry = new_schedule_trigger(
        17,
        "hourly",
        scheduled.astimezone(timezone(timedelta(hours=3))),
        scheduled + timedelta(seconds=1),
        ScheduleBackend.TEMPORAL,
    )
    assert first.trigger_id == retry.trigger_id
    assert first.trigger_id == (
        "29b272e3eeee0c67fe5b5a121f8f39d4b5d9625d656e8a0ec7f2b0f1615e2914"
    )


def test_temporal_priority_normalization_is_monotonic() -> None:
    assert [normalize_temporal_priority(value) for value in (-10, -1, 0, 1, 99)] == [
        1,
        2,
        3,
        4,
        5,
    ]

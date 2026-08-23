from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from pyservicelib_gorundebug.datasource.cron.apscheduler import (
    _PortableCronTrigger,
)


def test_portable_cron_skips_nonexistent_spring_wall_time() -> None:
    zone = ZoneInfo("America/New_York")
    trigger = _PortableCronTrigger("30 2 * * *", "America/New_York")

    next_fire = trigger.get_next_fire_time(
        None, datetime(2026, 3, 7, 3, tzinfo=zone)
    )

    assert next_fire is not None
    assert next_fire.astimezone(timezone.utc) == datetime(
        2026, 3, 9, 6, 30, tzinfo=timezone.utc
    )


def test_portable_cron_fires_first_ambiguous_fall_wall_time_once() -> None:
    zone = ZoneInfo("America/New_York")
    trigger = _PortableCronTrigger("30 1 * * *", "America/New_York")

    first = trigger.get_next_fire_time(
        None, datetime(2026, 10, 31, 2, tzinfo=zone)
    )
    assert first is not None
    second = trigger.get_next_fire_time(first, first)

    assert first.astimezone(timezone.utc) == datetime(
        2026, 11, 1, 5, 30, tzinfo=timezone.utc
    )
    assert second is not None
    assert second.astimezone(timezone.utc) == datetime(
        2026, 11, 2, 6, 30, tzinfo=timezone.utc
    )

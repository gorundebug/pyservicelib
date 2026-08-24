from datetime import datetime, timezone

import pytest
from pyservicelib_gorundebug.datasource.cron.apscheduler import (
    _PortableCronTrigger,
)


def test_portable_cron_calculates_utc_occurrence() -> None:
    trigger = _PortableCronTrigger("30 2 * * *", "UTC")

    next_fire = trigger.get_next_fire_time(
        None, datetime(2026, 3, 7, 3, tzinfo=timezone.utc)
    )

    assert next_fire is not None
    assert next_fire == datetime(
        2026, 3, 8, 2, 30, tzinfo=timezone.utc
    )


def test_portable_cron_rejects_non_utc_timezone() -> None:
    with pytest.raises(ValueError, match="timezone must be UTC"):
        _PortableCronTrigger("30 2 * * *", "Europe/Berlin")

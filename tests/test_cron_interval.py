import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from apscheduler.events import EVENT_JOB_MAX_INSTANCES, EVENT_JOB_SUBMITTED, JobSubmissionEvent
from apscheduler.job import Job

from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from pyservicelib_gorundebug.api.models.schedule_overlap_policy import ScheduleOverlapPolicy
from pyservicelib_gorundebug.datasource.cron.apscheduler import _CronDataSource, _CronEndpoint, _portable_cron_trigger
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.schedule_interval import parse_local_schedule_interval


def interval_source(handler, policy=ScheduleMissedRunPolicy.FIREONCE):
    connector = SimpleNamespace(id=1, name="interval", type=DataConnectorType.Cron)
    config = SimpleNamespace(
        id=2, name="sweep", enabled=True, schedule="@every 100ms", timezone="UTC",
        missed_run_policy=policy, overlap_policy=ScheduleOverlapPolicy.SKIP,
    )
    metrics = Mock()
    metrics.enabled = False
    environment = SimpleNamespace(
        config=SimpleNamespace(get_data_connector_by_id=lambda _: connector,
                               get_endpoint_config_by_id=lambda _: config),
        metrics=metrics, log=Mock(), tracing=None,
    )
    source = _CronDataSource(1, environment)
    endpoint = _CronEndpoint(source, 2)
    endpoint._consumer = SimpleNamespace(on_trigger=handler)
    source.add_endpoint(endpoint)
    return source, endpoint


@pytest.mark.parametrize("milliseconds", [10, 100, 1000, 86400000])
def test_interval_parser(milliseconds):
    assert parse_local_schedule_interval(f"@every {milliseconds}ms") == timedelta(milliseconds=milliseconds)
    assert parse_local_schedule_interval("*/5 * * * *") is None


@pytest.mark.parametrize("value", ["@every", "@every 9ms", "@every 86400001ms", "@every 010ms",
                                  "@every +10ms", "@every -10ms", "@every 1s", "@every 0.1s",
                                  "@every  100ms", "@every 100ms ", "@everywhere 100ms"])
def test_interval_rejects_noncanonical_values(value):
    with pytest.raises(ValueError, match="local interval requires"):
        parse_local_schedule_interval(value)


def test_interval_trigger_retains_millisecond_anchor():
    now = datetime(2026, 10, 27, 12, 0, 0, 123456, tzinfo=timezone.utc)
    trigger = _portable_cron_trigger("@every 100ms", "UTC", now=now)
    first = trigger.get_next_fire_time(None, now)
    assert first == now.replace(microsecond=123000) + timedelta(milliseconds=100)
    assert trigger.get_next_fire_time(first, first) == first + timedelta(milliseconds=100)
    with pytest.raises(ValueError, match="timezone must be UTC"):
        _portable_cron_trigger("@every 100ms", "Europe/Moscow", now=now)


@pytest.mark.parametrize("milliseconds", [10, 100, 86400000])
def test_native_scheduler_backlog_is_bounded_before_coalescing(milliseconds):
    start = datetime(2026, 10, 30, tzinfo=timezone.utc)
    trigger = _portable_cron_trigger(f"@every {milliseconds}ms", "UTC", now=start)
    job = Job.__new__(Job)
    job.trigger = trigger
    job.next_run_time = trigger.start_date
    job.coalesce = True
    now = trigger.start_date + 1_000_000 * trigger.interval
    # Exercise APScheduler enumeration itself, not a fabricated submission event.
    due = job._get_run_times(now)
    assert due == [trigger.start_date, now]
    assert trigger.get_next_fire_time(due[-1], now) == now + trigger.interval
    assert job._get_run_times(trigger.start_date) == [trigger.start_date]


@pytest.mark.parametrize("policy", [ScheduleMissedRunPolicy.SKIP, ScheduleMissedRunPolicy.FIREONCE])
def test_interval_coalesces_or_skips_backlog_and_accounts_for_overlap(policy):
    _, endpoint = interval_source(None, policy)
    step = timedelta(milliseconds=100)
    first = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    endpoint._interval_step, endpoint._interval_next = step, first

    def submit(code, when):
        endpoint._on_submission(JobSubmissionEvent(code, endpoint._job_id, "default", [when]))

    latest = first + 9999 * step
    submit(EVENT_JOB_SUBMITTED, latest)
    assert list(endpoint._scheduled) == [None if policy == ScheduleMissedRunPolicy.SKIP else latest]
    endpoint._scheduled.clear()
    submit(EVENT_JOB_MAX_INSTANCES, latest + step)
    assert not endpoint._scheduled
    submit(EVENT_JOB_SUBMITTED, latest + 2 * step)
    assert list(endpoint._scheduled) == [latest + 2 * step]
    endpoint._scheduled.clear()
    submit(EVENT_JOB_SUBMITTED, latest + step)
    assert list(endpoint._scheduled) == [None]
    assert endpoint._interval_next == latest + 3 * step


@pytest.mark.asyncio
async def test_real_interval_scheduler_and_shutdown():
    received = asyncio.Queue()

    async def handler(trigger):
        await received.put(trigger)

    source, endpoint = interval_source(handler)
    owner = Context(timedelta(seconds=5))
    try:
        await source.start(owner)
        values = [await asyncio.wait_for(received.get(), 2) for _ in range(3)]
        for previous, current in zip(values, values[1:]):
            delta = current.scheduled_at - previous.scheduled_at
            assert delta >= timedelta(milliseconds=100)
            assert delta % timedelta(milliseconds=100) == timedelta(0)
            assert current.trigger_id != previous.trigger_id
        owner.cancel()
        await source.stop(Context(timedelta(seconds=1)))
        assert source._scheduler is None
        assert not endpoint._active
        assert not endpoint._scheduled
    finally:
        owner.cancel()
        await source.stop(Context(timedelta(seconds=1)))

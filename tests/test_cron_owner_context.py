import asyncio
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from pyservicelib_gorundebug.api.models.schedule_overlap_policy import ScheduleOverlapPolicy
from pyservicelib_gorundebug.datasource.cron.apscheduler import (
    AsyncIOScheduler, _CronDataSource, _CronEndpoint,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled, request_context_error, request_context_scope,
    request_deadline, stream_id_from_context,
)
from pyservicelib_gorundebug.runtime.pool._scheduling import ContextWatches, make_task


def test_context_notifications_inherit_cancel_and_unsubscribe() -> None:
    parent = Context()
    child = parent.child()
    callback = Mock()
    remove = child.add_cancel_callback(callback)
    parent.cancel()
    child.cancel()
    callback.assert_called_once_with()
    remove()
    assert not parent._Context__cancel_callbacks
    assert not child._Context__cancel_callbacks
    immediate = Mock()
    child.add_cancel_callback(immediate)()
    immediate.assert_called_once_with()
    other = Context()
    detached = Mock()
    other.add_cancel_callback(detached)()
    other.cancel()
    detached.assert_not_called()
    independent_parent = Context()
    independent_parent.child().cancel()
    assert not independent_parent.cancelled


@pytest.mark.parametrize("registration_depth", [0, 1])
@pytest.mark.parametrize("threaded", [False, True])
def test_cancellation_during_registration_removes_late_ancestor_callbacks(
    registration_depth, threaded,
) -> None:
    root = Context()
    middle = root.child()
    child = middle.child()
    chain = [child, middle, root]
    callback = Mock()

    class CancelDuringRegistration(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            if threaded:
                worker = threading.Thread(target=child.cancel)
                worker.start()
                worker.join(timeout=1)
                assert not worker.is_alive()
            else:
                child.cancel()

    chain[registration_depth]._Context__cancel_callbacks = CancelDuringRegistration()
    remove = child.add_cancel_callback(callback)
    callback.assert_called_once_with()
    for context in chain:
        assert not context._Context__cancel_callbacks
        context.cancel()
    callback.assert_called_once_with()
    remove()


def test_simultaneous_parent_and_child_cancel_notify_once() -> None:
    for _ in range(16):
        parent = Context()
        child = parent.child()
        called = []
        remove = child.add_cancel_callback(lambda: called.append(True))
        ready = threading.Barrier(3)

        def cancel(context):
            ready.wait(timeout=2)
            context.cancel()

        workers = [threading.Thread(target=cancel, args=(context,)) for context in (parent, child)]
        for worker in workers:
            worker.start()
        ready.wait(timeout=2)
        for worker in workers:
            worker.join(timeout=2)
            assert not worker.is_alive()
        assert called == [True]
        assert not parent._Context__cancel_callbacks
        assert not child._Context__cancel_callbacks
        remove()


@pytest.mark.asyncio
async def test_owner_cancellation_reaches_queued_context_after_scope_exit() -> None:
    owner = Context()
    previous_deadline = request_deadline.get()
    previous_cancelled = request_cancelled.get()
    with request_context_scope(owner.child()):
        queued = make_task(lambda: None, (), {})
    assert request_deadline.get() is previous_deadline
    assert request_cancelled.get() is previous_cancelled
    watches = ContextWatches()
    notified = asyncio.Event()
    watches.add(queued, lambda _: notified.set())
    try:
        await asyncio.sleep(0)
        owner.cancel()
        await asyncio.wait_for(notified.wait(), 1)
        assert queued.cancelled_event.is_set()
        assert queued.context.run(request_context_error) == "request cancelled"
    finally:
        await watches.close()
    assert not owner._Context__cancel_callbacks


@pytest.mark.asyncio
async def test_owner_cancellation_notification_crosses_threads_without_polling() -> None:
    owner = Context()
    with request_context_scope(owner.child()):
        event = request_cancelled.get()
        waiter = asyncio.create_task(event.wait())
        await asyncio.sleep(0)
        await asyncio.to_thread(owner.cancel)
        assert await asyncio.wait_for(waiter, 1)
    assert not owner._Context__cancel_callbacks


@pytest.mark.asyncio
async def test_owner_deadline_reaches_existing_pool_deadline_watches() -> None:
    owner = Context(timedelta(milliseconds=20))
    watches = ContextWatches()
    notified = asyncio.Event()
    with request_context_scope(owner.child()):
        queued = make_task(lambda: None, (), {})
        watches.add(queued, lambda _: notified.set())
    try:
        await asyncio.wait_for(notified.wait(), 1)
        await asyncio.sleep(0.01)
        assert owner.is_expired
        assert queued.context.run(request_context_error) == "deadline exceeded"
    finally:
        await watches.close()
    assert not owner._Context__cancel_callbacks


def cron_source(handler):
    connector = SimpleNamespace(id=1, name="owner-cron", type=DataConnectorType.Cron)
    config = SimpleNamespace(
        id=2, name="scheduled", enabled=True, schedule="* * * * *", timezone="UTC",
        missed_run_policy=ScheduleMissedRunPolicy.FIREONCE,
        overlap_policy=ScheduleOverlapPolicy.SKIP,
    )
    metrics = Mock()
    metrics.enabled = False
    environment = SimpleNamespace(
        config=SimpleNamespace(
            get_data_connector_by_id=lambda _: connector,
            get_endpoint_config_by_id=lambda _: config,
        ),
        metrics=metrics, log=Mock(), tracing=None,
    )
    source = _CronDataSource(1, environment)
    endpoint = _CronEndpoint(source, 2)
    endpoint._consumer = SimpleNamespace(on_trigger=handler)
    source.add_endpoint(endpoint)
    return source, endpoint


async def fire_now(source, endpoint, owner):
    await source.start(owner)
    source._scheduler.modify_job(endpoint._job_id, next_run_time=datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_real_cron_inherits_owner_and_timeout_keeps_accepted_work_owned() -> None:
    captured = asyncio.get_running_loop().create_future()
    release = asyncio.Event()

    async def handler(_):
        captured.set_result((make_task(lambda: None, (), {}), stream_id_from_context()))
        await release.wait()

    owner = Context(timedelta(seconds=5))
    source, endpoint = cron_source(handler)
    try:
        await fire_now(source, endpoint, owner)
        queued, stream_id = await asyncio.wait_for(captured, 2)
        assert stream_id
        assert queued.deadline is not None
        assert 0 < (queued.deadline - datetime.now(timezone.utc)).total_seconds() <= 5
        owner.cancel()
        assert queued.cancelled_event.is_set()
        with pytest.raises(TimeoutError, match="shutdown deadline"):
            await source.stop(Context(timedelta(milliseconds=10)))
        assert not source._shutdown.done()
        assert endpoint._active
        with pytest.raises(RuntimeError, match="still draining"):
            await source.start(Context())
        release.set()
        await source.stop(Context(timedelta(seconds=1)))
        assert not source._started
        assert not endpoint._active
        assert source._scheduler is None
    finally:
        release.set()
        await source.stop(Context(timedelta(seconds=1)))


@pytest.mark.asyncio
async def test_real_cron_aborted_stop_retains_shared_drain_for_retry() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(_):
        entered.set()
        await release.wait()

    source, endpoint = cron_source(handler)
    try:
        await fire_now(source, endpoint, Context())
        await asyncio.wait_for(entered.wait(), 2)
        first = asyncio.create_task(source.stop(Context()))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not source._shutdown.done()
        cancelled = Context()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError, match="shutdown context"):
            await source.stop(cancelled)
        assert not source._shutdown.done()
        release.set()
        await source.stop(Context(timedelta(seconds=1)))
        assert not endpoint._active
    finally:
        release.set()
        await source.stop(Context(timedelta(seconds=1)))


@pytest.mark.asyncio
async def test_cron_rejects_cancelled_owner_before_scheduler_creation() -> None:
    source, _ = cron_source(Mock())
    owner = Context()
    owner.cancel()
    with pytest.raises(RuntimeError, match="cancelled or expired"):
        await source.start(owner)
    assert source._scheduler is None
    assert not source._started


@pytest.mark.asyncio
async def test_cron_startup_rollback_receives_the_same_owner(monkeypatch) -> None:
    source, _ = cron_source(Mock())
    owner = Context()
    stopped = []
    real_stop = source.stop

    async def capture_stop(context):
        stopped.append(context)
        await real_stop(context)

    def fail_start(_):
        raise RuntimeError("scheduler startup failure")

    monkeypatch.setattr(source, "stop", capture_stop)
    monkeypatch.setattr(AsyncIOScheduler, "start", fail_start)
    with pytest.raises(RuntimeError, match="scheduler startup failure"):
        await source.start(owner)
    assert stopped == [owner]
    assert source._scheduler is None
    assert not source._started

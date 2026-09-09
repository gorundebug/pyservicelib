#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import asyncio
import sys
import pytest
import os
from pathlib import Path
from datetime import timedelta, datetime
from unittest.mock import MagicMock

from pyservicelib_gorundebug.runtime.pool import (
    PoolAlreadyStartedError,
    PoolCancelledError,
    PoolStoppedError,
    make_delay_pool,
)
from pyservicelib_gorundebug.runtime.pool.taskpool import TaskPoolImpl
from pyservicelib_gorundebug.runtime.pool.prioritytaskpool import PriorityTaskPoolImpl
from pyservicelib_gorundebug.runtime.pool.delaypool import DelayPoolImpl
from pyservicelib_gorundebug.runtime.pool.threadpool import AsyncThreadPoolExecutor, AsyncFuture
from pyservicelib_gorundebug.runtime.serviceapp import ServiceAppLoader
from pyservicelib_gorundebug.runtime.context import default_context
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled,
    request_deadline,
    request_stream_id,
)
from pyservicelib_gorundebug.runtime.config import  ConfigSettings
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as MetricsRecorder

from .mockservice import MockService, MockServiceConfig, MockServiceDependency


def _make_env(executors_count: int = 1, delay_executors: int = 1) -> MagicMock:
    env = MagicMock()
    pool_cfg = MagicMock()
    pool_cfg.executors_count = executors_count
    env.config.get_pool_by_name.return_value = pool_cfg
    env.service_config.delay_executors = delay_executors
    return env


def _make_metrics_env(executors_count: int = 1) -> tuple[MagicMock, MetricsRecorder]:
    env = _make_env(executors_count)
    metrics = MetricsRecorder()
    env.metrics = metrics
    env.service_config.name = 'test-svc'
    return env, metrics


@pytest.mark.asyncio
async def test_delay_pool():
   config_dir = str(Path(__file__).parent / "mockservice" / "config")
   sys.argv = [sys.argv[0],
               "--config", f"{config_dir}/config.yaml"]
   delays: list[int] = [1000, 5000, 1200, 3000, 1500, 4000, 1350, 900, 100, 500, 500, 500, 500, 500, 500, 500]
   recorded_delays: list[int] = []

   service = await ServiceAppLoader[MockService, MockServiceConfig]().load(
      "IncomeService", MockServiceDependency(), ConfigSettings())
   ctx = default_context()

   delay_pool = make_delay_pool(service)
   await delay_pool.start(ctx)

   async def task_with_delay(value: int, start_time: datetime):
      time_difference = int((datetime.now() - start_time).total_seconds() * 1000)
      assert abs(time_difference - value) <= 5
      recorded_delays.append(value)

   for delay in delays:
      await delay_pool.add_task(timedelta(milliseconds=delay), task_with_delay, delay, datetime.now())

   await delay_pool.stop(ctx)
   delays.sort()
   assert recorded_delays == delays

   await service.stop(ctx)
   await service.release()

@pytest.mark.asyncio
async def test_async_pool():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   start_time = datetime.now()

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50

   pool.shutdown()

@pytest.mark.asyncio
async def test_async_pool_add_task_without_block():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   start_time = datetime.now()

   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50

   pool.shutdown()

@pytest.mark.asyncio
async def test_async_pool_shutdown():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   start_time = datetime.now()

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   pool.shutdown()
   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50


# ========== TaskPoolImpl: lifecycle + semantics ==========

@pytest.mark.asyncio
async def test_task_pool_init_missing_config():
    env = MagicMock()
    env.config.get_pool_by_name.return_value = None
    with pytest.raises(ValueError):
        TaskPoolImpl("missing", env)


@pytest.mark.asyncio
async def test_task_pool_start_once():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    with pytest.raises(PoolAlreadyStartedError):
        await pool.start(ctx)
    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_task_pool_start_after_stop_raises():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.start(ctx)


@pytest.mark.asyncio
async def test_task_pool_stop_idempotent():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    await pool.stop(ctx)  # must not raise


@pytest.mark.asyncio
async def test_task_pool_add_task_after_stop_raises():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.add_task(asyncio.sleep, 0)


@pytest.mark.asyncio
async def test_task_pool_tasks_execute():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results = []

    async def job(x):
        results.append(x)

    await pool.add_task(job, 1)
    await pool.add_task(job, 2)
    await pool.add_task(job, 3)
    await pool.stop(ctx)

    assert results == [1, 2, 3]


@pytest.mark.asyncio
async def test_task_pool_executor_metrics():
    env, metrics = _make_metrics_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    started = asyncio.Event()
    release = asyncio.Event()

    async def job():
        started.set()
        await release.wait()

    await pool.start(ctx)
    await pool.add_task(job)
    await started.wait()

    labels = {'service': 'test-svc', 'name': 'p'}
    assert metrics.gauge('task_pool_executors_target', labels).value() == 1
    assert metrics.gauge('task_pool_executors_allocated', labels).value() == 1
    assert metrics.gauge('task_pool_executors_busy', labels).value() == 1

    release.set()
    await pool.stop(ctx)
    assert metrics.gauge('task_pool_executors_allocated', labels).value() == 0
    assert metrics.gauge('task_pool_executors_busy', labels).value() == 0


@pytest.mark.asyncio
async def test_task_pool_exception_does_not_crash():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results = []

    async def bad_job():
        raise RuntimeError("oops")

    async def good_job():
        results.append("ok")

    await pool.add_task(bad_job)
    await pool.add_task(good_job)
    await pool.stop(ctx)

    assert results == ["ok"]


# ========== PriorityTaskPoolImpl: lifecycle + tie-breaking ==========


@pytest.mark.asyncio
async def test_priority_pool_executor_metrics():
    env, metrics = _make_metrics_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    started = asyncio.Event()
    release = asyncio.Event()

    async def job():
        started.set()
        await release.wait()

    await pool.start(ctx)
    await pool.add_task(0, job)
    await started.wait()

    labels = {'service': 'test-svc', 'name': 'p'}
    assert metrics.gauge('priority_task_pool_executors_target', labels).value() == 1
    assert metrics.gauge('priority_task_pool_executors_allocated', labels).value() == 1
    assert metrics.gauge('priority_task_pool_executors_busy', labels).value() == 1

    release.set()
    await pool.stop(ctx)
    assert metrics.gauge('priority_task_pool_executors_allocated', labels).value() == 0
    assert metrics.gauge('priority_task_pool_executors_busy', labels).value() == 0

@pytest.mark.asyncio
async def test_priority_pool_start_once():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    with pytest.raises(PoolAlreadyStartedError):
        await pool.start(ctx)
    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_priority_pool_start_after_stop_raises():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.start(ctx)


@pytest.mark.asyncio
async def test_priority_pool_stop_idempotent():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_priority_pool_add_after_stop_raises():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.add_task(0, asyncio.sleep, 0)


@pytest.mark.asyncio
async def test_priority_pool_order():
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results = []
    barrier = asyncio.Event()

    async def blocker():
        await barrier.wait()

    async def job(label):
        results.append(label)

    await pool.add_task(0, blocker)
    await pool.add_task(3, job, "low")
    await pool.add_task(1, job, "high")
    await pool.add_task(2, job, "mid")

    barrier.set()
    await pool.stop(ctx)

    assert results == ["high", "mid", "low"]


@pytest.mark.asyncio
async def test_priority_pool_equal_priority_no_type_error():
    # Before fix, equal priorities caused TypeError when comparing callables
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results = []

    async def job(x):
        results.append(x)

    await pool.add_task(5, job, 1)
    await pool.add_task(5, job, 2)
    await pool.add_task(5, job, 3)
    await pool.stop(ctx)

    assert sorted(results) == [1, 2, 3]


@pytest.mark.asyncio
async def test_priority_pool_equal_priority_fifo():
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results = []
    barrier = asyncio.Event()

    async def blocker():
        await barrier.wait()

    async def job(x):
        results.append(x)

    await pool.add_task(0, blocker)
    await pool.add_task(5, job, "a")
    await pool.add_task(5, job, "b")
    await pool.add_task(5, job, "c")

    barrier.set()
    await pool.stop(ctx)

    assert results == ["a", "b", "c"]


# ========== DelayPoolImpl: lifecycle + deadline cap ==========

@pytest.mark.asyncio
async def test_delay_pool_start_once():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)
    with pytest.raises(PoolAlreadyStartedError):
        await pool.start(ctx)
    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_delay_pool_start_after_stop_raises():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.start(ctx)


@pytest.mark.asyncio
async def test_delay_pool_stop_idempotent():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_delay_pool_add_after_stop_raises():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)
    await pool.stop(ctx)
    with pytest.raises(PoolStoppedError):
        await pool.add_task(timedelta(seconds=1), asyncio.sleep, 0)


@pytest.mark.asyncio
async def test_delay_pool_equal_time_no_type_error():
    # Before fix, equal execute_times caused TypeError when comparing callables
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    results = []

    async def job(x):
        results.append(x)

    delay = timedelta(milliseconds=30)
    await pool.add_task(delay, job, 1)
    await pool.add_task(delay, job, 2)
    await pool.add_task(delay, job, 3)
    await pool.stop(ctx)

    assert sorted(results) == [1, 2, 3]


@pytest.mark.asyncio
async def test_delay_pool_request_deadline_caps_delay():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    executed_at = None

    async def job():
        nonlocal executed_at
        executed_at = datetime.now()

    enqueued_at = datetime.now()
    deadline = enqueued_at + timedelta(milliseconds=60)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(timedelta(milliseconds=500), job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)

    assert executed_at is not None
    elapsed = (executed_at - enqueued_at).total_seconds()
    # Must execute near deadline (~60ms), not at 500ms
    assert elapsed < 0.4, f"deadline not honoured: task ran at {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_delay_pool_no_deadline_uses_full_delay():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    executed_at = None

    async def job():
        nonlocal executed_at
        executed_at = datetime.now()

    enqueued_at = datetime.now()
    await pool.add_task(timedelta(milliseconds=60), job)
    await pool.stop(ctx)

    assert executed_at is not None
    elapsed = (executed_at - enqueued_at).total_seconds()
    assert elapsed >= 0.05, f"task ran too early: {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_delay_pool_context_cancel_runs_immediately():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    executed = asyncio.Event()
    cancelled = asyncio.Event()

    async def job():
        executed.set()

    token = request_cancelled.set(cancelled)
    try:
        await pool.add_task(timedelta(seconds=10), job)
    finally:
        request_cancelled.reset(token)

    cancelled.set()
    await asyncio.wait_for(executed.wait(), timeout=0.5)
    await pool.stop(ctx)

    assert executed.is_set()


@pytest.mark.asyncio
async def test_delay_pool_preserves_complete_request_context():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    captured_stream_id = None

    async def job():
        nonlocal captured_stream_id
        captured_stream_id = request_stream_id.get()

    token = request_stream_id.set("request-42")
    try:
        await pool.add_task(timedelta(milliseconds=10), job)
    finally:
        request_stream_id.reset(token)

    await pool.stop(ctx)

    assert captured_stream_id == "request-42"


# ========== AfterFunc / request_deadline context propagation ==========

@pytest.mark.asyncio
async def test_task_pool_rejects_expired_deadline():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    past = datetime.now() - timedelta(milliseconds=10)
    token = request_deadline.set(past)
    try:
        with pytest.raises(PoolCancelledError):
            await pool.add_task(asyncio.sleep, 0)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_task_pool_cancel_moves_to_front_without_bypassing_executor():
    """Mirror Go TestTaskPool_CancelMovesToFront."""
    env = _make_env(executors_count=1)
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results: list[str] = []
    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def append(name: str):
        results.append(name)
        if len(results) == 3:
            executed.set()

    await pool.add_task(blocker)
    await asyncio.sleep(0)
    await pool.add_task(append, "first")
    await pool.add_task(append, "second")

    cancelled = asyncio.Event()
    token = request_cancelled.set(cancelled)
    try:
        await pool.add_task(append, "cancelled")
    finally:
        request_cancelled.reset(token)

    cancelled.set()
    await asyncio.sleep(0.03)

    # Cancellation changes queue order, but must not create a hidden executor.
    assert not executed.is_set()
    assert results == []

    blocker_release.set()
    await asyncio.wait_for(executed.wait(), timeout=1.0)
    await pool.stop(ctx)

    assert results == ["cancelled", "first", "second"]


@pytest.mark.asyncio
async def test_task_pool_deadline_moves_to_front_without_bypassing_executor():
    env = _make_env(executors_count=1)
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results: list[str] = []
    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def append(name: str):
        results.append(name)
        executed.set()

    await pool.add_task(blocker)
    await asyncio.sleep(0)
    await pool.add_task(append, "normal")

    deadline = datetime.now() + timedelta(milliseconds=60)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(append, "deadline")
    finally:
        request_deadline.reset(token)

    await asyncio.sleep(0.1)
    assert not executed.is_set()

    blocker_release.set()
    await asyncio.wait_for(executed.wait(), timeout=1.0)
    await pool.stop(ctx)

    assert results == ["deadline", "normal"]


@pytest.mark.asyncio
async def test_task_pool_restores_deadline_in_executor():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    captured = None

    async def job():
        nonlocal captured
        captured = request_deadline.get()

    deadline = datetime.now() + timedelta(seconds=10)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)

    assert captured == deadline


@pytest.mark.asyncio
async def test_task_pool_no_double_execution():
    env = _make_env(executors_count=1)
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    count = 0

    async def job():
        nonlocal count
        count += 1

    # Deadline far in future — after_func won't fire before normal dequeue
    deadline = datetime.now() + timedelta(seconds=10)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)
    assert count == 1


@pytest.mark.asyncio
async def test_priority_pool_restores_deadline_in_executor():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    captured = None

    async def job():
        nonlocal captured
        captured = request_deadline.get()

    deadline = datetime.now() + timedelta(seconds=10)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(0, job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)
    assert captured == deadline


@pytest.mark.asyncio
async def test_priority_pool_cancel_promotes_without_bypassing_executor():
    """Mirror Go TestPriorityTaskPool_CancelPromotion."""
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results: list[str] = []
    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def append(name: str):
        results.append(name)
        if len(results) == 2:
            executed.set()

    await pool.add_task(0, blocker)
    await asyncio.sleep(0)

    cancelled = asyncio.Event()
    token = request_cancelled.set(cancelled)
    try:
        await pool.add_task(100, append, "low")
    finally:
        request_cancelled.reset(token)
    await pool.add_task(1, append, "high")

    cancelled.set()
    await asyncio.sleep(0.03)
    assert not executed.is_set()
    assert results == []

    blocker_release.set()
    await asyncio.wait_for(executed.wait(), timeout=1.0)
    await pool.stop(ctx)

    assert results == ["low", "high"]


@pytest.mark.asyncio
async def test_priority_pool_deadline_promotes_without_bypassing_executor():
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    results: list[str] = []
    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def append(name: str):
        results.append(name)
        executed.set()

    await pool.add_task(0, blocker)
    await asyncio.sleep(0)
    await pool.add_task(1, append, "high")

    deadline = datetime.now() + timedelta(milliseconds=60)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(99, append, "deadline")
    finally:
        request_deadline.reset(token)

    await asyncio.sleep(0.1)
    assert not executed.is_set()

    blocker_release.set()
    await asyncio.wait_for(executed.wait(), timeout=1.0)
    await pool.stop(ctx)

    assert results == ["deadline", "high"]


@pytest.mark.asyncio
async def test_delay_pool_after_func_fires_before_execute_time():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    executed_at = None

    async def job():
        nonlocal executed_at
        executed_at = datetime.now()

    enqueued_at = datetime.now()
    req_deadline = enqueued_at + timedelta(milliseconds=60)
    token = request_deadline.set(req_deadline)
    try:
        await pool.add_task(timedelta(milliseconds=400), job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)

    assert executed_at is not None
    elapsed = (executed_at - enqueued_at).total_seconds()
    # after_func fires at ~60ms, not at 400ms execute_time
    assert elapsed < 0.25, f"after_func didn't fire early: ran at {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_delay_pool_restores_deadline_in_executor():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    captured = None

    async def job():
        nonlocal captured
        captured = request_deadline.get()

    deadline = datetime.now() + timedelta(seconds=10)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(timedelta(milliseconds=30), job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)
    assert captured == deadline


@pytest.mark.asyncio
async def test_delay_pool_no_double_execution():
    # Deadline before execute_time: after_func fires, queue entry must be skipped
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    count = 0

    async def job():
        nonlocal count
        count += 1

    req_deadline = datetime.now() + timedelta(milliseconds=50)
    token = request_deadline.set(req_deadline)
    try:
        await pool.add_task(timedelta(milliseconds=200), job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)
    assert count == 1


# ========== TaskPool + PriorityTaskPool: admission before start and shutdown waits ==========

@pytest.mark.asyncio
async def test_task_pool_accepts_before_start():
    pool = TaskPoolImpl("p", _make_env())
    completed = asyncio.Event()
    async def callback():
        completed.set()
    await pool.add_task(callback)
    await pool.start(default_context())
    await pool.stop(default_context())
    assert completed.is_set()


@pytest.mark.asyncio
async def test_priority_pool_accepts_before_start():
    pool = PriorityTaskPoolImpl("p", _make_env())
    completed = asyncio.Event()
    async def callback():
        completed.set()
    await pool.add_task(0, callback)
    await pool.start(default_context())
    await pool.stop(default_context())
    assert completed.is_set()


@pytest.mark.asyncio
async def test_task_pool_stop_drains_context_watched_task():
    env = _make_env(executors_count=1)
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def job():
        await asyncio.sleep(0.02)
        executed.set()

    await pool.add_task(blocker)

    deadline = datetime.now() + timedelta(milliseconds=50)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(job)
    finally:
        request_deadline.reset(token)

    blocker_release.set()
    await pool.stop(ctx)
    assert executed.is_set(), "stop() returned before queued task finished"


@pytest.mark.asyncio
async def test_task_pool_rejects_expired_deadline_without_leaking_work():
    env = _make_env()
    pool = TaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    past = datetime.now() - timedelta(milliseconds=10)
    token = request_deadline.set(past)
    try:
        with pytest.raises(PoolCancelledError):
            await pool.add_task(asyncio.sleep, 0)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_priority_pool_stop_drains_context_watched_task():
    env = _make_env(executors_count=1)
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    executed = asyncio.Event()
    blocker_release = asyncio.Event()

    async def blocker():
        await blocker_release.wait()

    async def job():
        await asyncio.sleep(0.02)
        executed.set()

    await pool.add_task(0, blocker)

    deadline = datetime.now() + timedelta(milliseconds=50)
    token = request_deadline.set(deadline)
    try:
        await pool.add_task(99, job)
    finally:
        request_deadline.reset(token)

    blocker_release.set()
    await pool.stop(ctx)
    assert executed.is_set()


@pytest.mark.asyncio
async def test_priority_pool_rejects_expired_deadline_without_leaking_work():
    env = _make_env()
    pool = PriorityTaskPoolImpl("p", env)
    ctx = default_context()
    await pool.start(ctx)

    past = datetime.now() - timedelta(milliseconds=10)
    token = request_deadline.set(past)
    try:
        with pytest.raises(PoolCancelledError):
            await pool.add_task(0, asyncio.sleep, 0)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)


@pytest.mark.asyncio
async def test_delay_pool_accepts_before_start():
    pool = DelayPoolImpl(_make_env())
    completed = asyncio.Event()
    async def callback():
        completed.set()
    await pool.add_task(timedelta(milliseconds=1), callback)
    await pool.start(default_context())
    await pool.stop(default_context())
    assert completed.is_set()


@pytest.mark.asyncio
async def test_delay_pool_stop_waits_for_context_expedited_execution():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    executed = asyncio.Event()

    async def job():
        await asyncio.sleep(0.02)
        executed.set()

    # Deadline in 30ms expedites execution from the 500ms delay.
    req_deadline = datetime.now() + timedelta(milliseconds=30)
    token = request_deadline.set(req_deadline)
    try:
        await pool.add_task(timedelta(milliseconds=500), job)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)
    assert executed.is_set(), "stop() returned before expedited task finished"


@pytest.mark.asyncio
async def test_delay_pool_rejects_expired_deadline_without_leaking_work():
    env = _make_env()
    pool = DelayPoolImpl(env)
    ctx = default_context()
    await pool.start(ctx)

    past = datetime.now() - timedelta(milliseconds=10)
    token = request_deadline.set(past)
    try:
        with pytest.raises(PoolCancelledError):
            await pool.add_task(timedelta(milliseconds=500), asyncio.sleep, 0)
    finally:
        request_deadline.reset(token)

    await pool.stop(ctx)


@pytest.mark.parametrize('kind', ['fifo', 'priority', 'delay'])
async def test_pool_independent_context_and_shared_shutdown(kind):
    from contextvars import ContextVar
    local = ContextVar('pool_regression', default='missing')
    env = _make_env(2)
    env.log.warn.side_effect = RuntimeError('reporter failed')
    pool = (DelayPoolImpl(env) if kind == 'delay' else
            TaskPoolImpl('test', env) if kind == 'fifo' else PriorityTaskPoolImpl('test', env))
    async def add(fn):
        if kind == 'delay':
            await pool.add_task(timedelta(), fn)
        elif kind == 'priority':
            await pool.add_task(0, fn)
        else:
            await pool.add_task(fn)
    gate = asyncio.Event()
    progress = asyncio.Event()
    seen = []
    tasks = []
    async def first():
        tasks.append(asyncio.current_task())
        seen.append(local.get())
        await gate.wait()
        seen.append(local.get())
    async def second():
        tasks.append(asyncio.current_task())
        await asyncio.sleep(0)
        seen.append(local.get())
        progress.set()
        raise asyncio.CancelledError()
    for value, fn in [('first', first), ('second', second)]:
        token = local.set(value)
        try:
            await add(fn)
        finally:
            local.reset(token)
    await pool.start(default_context())
    a = asyncio.create_task(pool.stop(default_context()))
    b = asyncio.create_task(pool.stop(default_context()))
    await asyncio.wait_for(progress.wait(), 1)
    assert not a.done() and not b.done()
    a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await a
    assert not b.done()
    gate.set()
    await asyncio.wait_for(b, 1)
    assert seen == ['first', 'second', 'first']
    assert tasks[0] is not tasks[1]
    await pool.stop(default_context())


@pytest.mark.parametrize('kind', ['fifo', 'priority'])
async def test_pool_zero_cpu_and_prestart_drain(kind, monkeypatch):
    monkeypatch.setattr(os, 'cpu_count', lambda: 2)
    env = _make_env(0)
    pool = TaskPoolImpl('test', env) if kind == 'fifo' else PriorityTaskPoolImpl('test', env)
    assert pool._target == 2
    ran = asyncio.Event()
    async def callback():
        ran.set()
    if kind == 'fifo':
        await pool.add_task(callback)
    else:
        await pool.add_task(0, callback)
    await asyncio.wait_for(pool.stop(default_context()), 1)
    assert ran.is_set()
    assert not pool._all_executors


@pytest.mark.parametrize('kind', ['fifo', 'priority', 'delay'])
async def test_pool_shared_cancellation_has_no_duplicates(kind):
    env = _make_env(1)
    pool = (DelayPoolImpl(env) if kind == 'delay' else
            TaskPoolImpl('test', env) if kind == 'fifo' else PriorityTaskPoolImpl('test', env))
    event = asyncio.Event()
    token = request_cancelled.set(event)
    seen = []
    async def callback(index):
        seen.append(index)
    try:
        for n in range(200):
            if kind == 'delay':
                await pool.add_task(timedelta(days=1), callback, n)
            elif kind == 'priority':
                await pool.add_task(n, callback, n)
            else:
                await pool.add_task(callback, n)
    finally:
        request_cancelled.reset(token)
    assert len(pool._watches.waiters) == 1
    event.set()
    assert not seen
    await asyncio.sleep(0)
    await asyncio.wait_for(pool.stop(default_context()), 1)
    assert sorted(seen) == list(range(200))
    assert not pool._queue
    assert not pool._watches.callbacks
    assert not pool._watches.groups
    assert not pool._watches.waiters


async def test_native_pool_cancelled_future_and_async_close():
    pool = AsyncThreadPoolExecutor(1)
    async def slow():
        await asyncio.sleep(.03)
        return 1
    future = pool.add_task(slow)
    waiter = asyncio.create_task(future.result())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    async def cancelled():
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await pool.add_task(cancelled).result()
    assert await pool.add_task(slow).result() == 1
    closing = asyncio.create_task(pool.aclose())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match='stopped'):
        pool.add_task(slow)
    await asyncio.wait_for(closing, 1)
    assert all(thread._loop.is_closed() for thread in pool.threads)
    assert all(not thread._thread.is_alive() for thread in pool.threads)


@pytest.mark.parametrize('kind', ['fifo', 'priority'])
async def test_pool_hot_shrink_and_stop_keep_concurrency_bound(kind):
    env = _make_env(3)
    pool = TaskPoolImpl('test', env) if kind == 'fifo' else PriorityTaskPoolImpl('test', env)
    gates = [asyncio.Event() for _ in range(3)]
    started = []
    queued = asyncio.Event()
    async def block(index):
        started.append(index)
        await gates[index].wait()
    async def callback():
        queued.set()
    async def add(fn, *args):
        if kind == 'fifo':
            await pool.add_task(fn, *args)
        else:
            await pool.add_task(0, fn, *args)
    await pool.start(default_context())
    for n in range(3):
        await add(block, n)
    await add(callback)
    while len(started) < 3:
        await asyncio.sleep(0)
    env.config.get_pool_by_name.return_value.executors_count = 1
    async with asyncio.timeout(2):
        while pool._target != 1:
            await asyncio.sleep(.01)
    stop = asyncio.create_task(pool.stop(default_context()))
    # Release the retained worker first: it must not start new work while retired
    # workers are still executing callbacks.
    gates[0].set()
    await asyncio.sleep(.01)
    assert not queued.is_set()
    gates[1].set()
    await asyncio.sleep(.01)
    assert not queued.is_set()
    gates[2].set()
    await asyncio.wait_for(stop, 1)
    assert queued.is_set()


async def test_delay_stop_timeout_does_not_advance_scheduled_work():
    pool = DelayPoolImpl(_make_env())
    seen = []
    loop = asyncio.get_running_loop()
    start = loop.time()
    async def callback():
        seen.append(loop.time() - start)
    await pool.add_task(timedelta(milliseconds=60), callback)
    ctx = default_context()
    ctx.cancel()
    stop = asyncio.create_task(pool.stop(ctx))
    await asyncio.sleep(.01)
    assert not stop.done()
    assert not seen
    await asyncio.wait_for(stop, 1)
    assert len(seen) == 1 and seen[0] >= .055

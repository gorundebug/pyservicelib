#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import pytest
from datetime import timedelta, datetime
from abc import ABC

from pyservicelib_gorundebug.runtime.store.hashmap import HashMapJoinStorage, Item, make_hashmap_storage
from pyservicelib_gorundebug.runtime.store.storage import JoinStorageConfig, StoreAlreadyStartedError, StoreStoppedError
from pyservicelib_gorundebug.runtime.store.joinstore import JoinStorageFactory
from pyservicelib_gorundebug.runtime.context import default_context, request_deadline
from pyservicelib_gorundebug.runtime.environment.metrics import (
    Int64Counter, Int64Gauge, MetricsScope, Metrics,
)


# ---------- stubs ----------

class _NoopScope(MetricsScope):
    def counter(self, name, help, labels): return _NoopCounter()
    def counter_vec(self, name, help): raise NotImplementedError
    def gauge(self, name, help, labels): return _NoopGauge()
    def gauge_vec(self, name, help): raise NotImplementedError
    def histogram(self, name, help, labels, *buckets): raise NotImplementedError
    def histogram_vec(self, name, help, *buckets): raise NotImplementedError
    def observable_float64_gauge(self, name, help, fn): pass


class _NoopCounter(Int64Counter):
    def inc(self): pass
    def add(self, v: int): pass


class _NoopGauge(Int64Gauge):
    def set(self, v: int): pass
    def inc(self): pass
    def dec(self): pass
    def add(self, delta: int): pass
    def sub(self, delta: int): pass


class _NoopMetrics(Metrics):
    def scope(self, prefix, labels): return _NoopScope()


class _MockServiceConfig:
    name = "test"


class _MockEnv:
    metrics = _NoopMetrics()
    service_config = _MockServiceConfig()


_ENV = _MockEnv()


class MockJoinStorageConfig(JoinStorageConfig):
    def __init__(self, ttl: timedelta, renew_ttl: bool = False, name: str = "test"):
        self._ttl = ttl
        self._renew_ttl = renew_ttl
        self._name = name

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    @property
    def renew_ttl(self) -> bool:
        return self._renew_ttl

    @property
    def name(self) -> str:
        return self._name


def make_storage(ttl_seconds: float = 3600, renew_ttl: bool = False) -> HashMapJoinStorage:
    cfg = MockJoinStorageConfig(ttl=timedelta(seconds=ttl_seconds), renew_ttl=renew_ttl)
    return HashMapJoinStorage(env=_ENV, cfg=cfg)  # type: ignore[arg-type]


# ---------- construction tests ----------

@pytest.mark.asyncio
async def test_construction_empty_storages():
    s = make_storage()
    assert s._current == {}
    assert s._prev == {}
    assert s._timer_task is None


@pytest.mark.asyncio
async def test_start_creates_rotation_task():
    s = make_storage()
    ctx = default_context()
    await s.start(ctx)
    assert s._timer_task is not None
    assert not s._timer_task.done()
    await s.stop(ctx)


@pytest.mark.asyncio
async def test_stop_cancels_rotation_task():
    s = make_storage()
    ctx = default_context()
    await s.start(ctx)
    await s.stop(ctx)
    assert s._timer_task is not None
    assert s._timer_task.done()


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    s = make_storage()
    ctx = default_context()
    await s.stop(ctx)  # must not raise


@pytest.mark.asyncio
async def test_start_twice_raises_already_started():
    s = make_storage()
    ctx = default_context()
    await s.start(ctx)
    with pytest.raises(StoreAlreadyStartedError):
        await s.start(ctx)
    await s.stop(ctx)


@pytest.mark.asyncio
async def test_start_after_stop_raises_stopped():
    s = make_storage()
    ctx = default_context()
    await s.start(ctx)
    await s.stop(ctx)
    with pytest.raises(StoreStoppedError):
        await s.start(ctx)


@pytest.mark.asyncio
async def test_stop_twice_is_noop():
    s = make_storage()
    ctx = default_context()
    await s.start(ctx)
    await s.stop(ctx)
    await s.stop(ctx)  # must not raise or re-cancel


@pytest.mark.asyncio
async def test_start_without_prior_start_raises_stopped():
    s = make_storage()
    ctx = default_context()
    # stop without start, then start → StoreStoppedError
    await s.stop(ctx)
    with pytest.raises(StoreStoppedError):
        await s.start(ctx)


# ---------- join_value: basic callback behaviour ----------

@pytest.mark.asyncio
async def test_join_value_callback_is_called():
    s = make_storage()
    called = False

    async def cb(values):
        nonlocal called
        called = True
        return False

    await s.join_value("k1", 0, "hello", cb)
    assert called


@pytest.mark.asyncio
async def test_join_value_callback_receives_value():
    s = make_storage()
    received = None

    async def cb(values):
        nonlocal received
        received = values
        return True

    await s.join_value("k1", 0, "hello", cb)
    assert received is not None
    assert len(received) >= 1
    assert received[0] == ["hello"]


@pytest.mark.asyncio
async def test_join_value_callback_true_removes_item():
    s = make_storage()

    async def cb(values):
        return True

    await s.join_value("k1", 0, 42, cb)
    assert "k1" not in s._current
    assert "k1" not in s._prev


@pytest.mark.asyncio
async def test_join_value_callback_false_keeps_item():
    s = make_storage()

    async def cb(values):
        return False

    await s.join_value("k1", 0, "v", cb)
    assert "k1" in s._current


@pytest.mark.asyncio
async def test_join_value_index_one_creates_two_slots():
    s = make_storage()
    received = None

    async def cb(values):
        nonlocal received
        received = values
        return False

    await s.join_value("k1", 1, "v", cb)
    assert received is not None
    assert len(received) >= 2
    assert received[1] == ["v"]


@pytest.mark.asyncio
async def test_join_value_accumulates_values_on_same_key():
    s = make_storage()
    results = []

    async def cb(values):
        results.append([list(v) for v in values])
        return False

    await s.join_value("k1", 0, "a", cb)
    await s.join_value("k1", 0, "b", cb)

    assert len(results) == 2
    assert results[0][0] == ["a"]
    assert results[1][0] == ["a", "b"]


@pytest.mark.asyncio
async def test_join_value_different_keys_are_independent():
    s = make_storage()

    async def cb(values):
        return False

    for key in ["x", "y", "z"]:
        await s.join_value(key, 0, 1, cb)

    assert len(s._current) == 3


@pytest.mark.asyncio
async def test_join_value_callback_true_allows_fresh_item_after_removal():
    s = make_storage()
    call_count = 0

    async def cb(values):
        nonlocal call_count
        call_count += 1
        return True

    await s.join_value("k1", 0, "v1", cb)
    await s.join_value("k1", 0, "v2", cb)

    assert call_count == 2


# ---------- TTL expiry tests ----------

@pytest.mark.asyncio
async def test_expired_item_in_current_is_replaced():
    s = make_storage(ttl_seconds=3600)
    loop = asyncio.get_event_loop()
    expired_item = Item[str](loop.time() - 1.0, 1)  # deadline 1 second ago
    s._current["k1"] = expired_item

    received = None

    async def cb(values):
        nonlocal received
        received = values
        return True

    await s.join_value("k1", 0, "fresh", cb)

    assert received is not None
    assert received[0] == ["fresh"]
    assert "k1" not in s._current
    assert "k1" not in s._prev


@pytest.mark.asyncio
async def test_expired_item_in_prev_is_discarded():
    s = make_storage(ttl_seconds=3600)
    loop = asyncio.get_event_loop()
    expired_item = Item[str](loop.time() - 1.0, 1)  # deadline 1 second ago
    s._prev["k2"] = expired_item

    received_fresh = False

    async def cb(values):
        nonlocal received_fresh
        received_fresh = True
        return True

    await s.join_value("k2", 0, "val", cb)
    assert received_fresh


@pytest.mark.asyncio
async def test_valid_item_in_prev_is_promoted_to_current():
    s = make_storage(ttl_seconds=3600)
    loop = asyncio.get_event_loop()
    valid_item = Item[str](loop.time() + 3600.0, 1)  # deadline 1 hour from now
    s._prev["k3"] = valid_item

    received = None

    async def cb(values):
        nonlocal received
        received = values
        return False

    await s.join_value("k3", 0, "from_prev", cb)

    assert received is not None
    assert "from_prev" in received[0]
    # Item promoted from prev to current
    assert "k3" in s._current
    assert "k3" not in s._prev


# ---------- renew_ttl tests ----------

@pytest.mark.asyncio
async def test_renew_ttl_updates_deadline():
    s = make_storage(ttl_seconds=10, renew_ttl=True)

    async def cb_first(values):
        return False

    await s.join_value("k1", 0, "v1", cb_first)
    item = s._current.get("k1")
    assert item is not None
    original_deadline = item.deadline

    await asyncio.sleep(0.01)

    async def cb_second(values):
        return False

    await s.join_value("k1", 0, "v2", cb_second)
    item_after = s._current.get("k1")
    assert item_after is not None
    assert item_after.deadline > original_deadline


@pytest.mark.asyncio
async def test_renew_ttl_moves_item_from_prev_to_current():
    s = make_storage(ttl_seconds=3600, renew_ttl=True)
    loop = asyncio.get_event_loop()
    valid_item = Item[str](loop.time() + 3600.0, 1)
    s._prev["k4"] = valid_item

    async def cb(values):
        return False  # trigger renewTTL path

    await s.join_value("k4", 0, "v", cb)

    # After renewTTL, item must be in current
    assert "k4" in s._current
    assert "k4" not in s._prev


# ---------- rotation tests (RotatingMap semantics) ----------

@pytest.mark.asyncio
async def test_rotation_moves_current_to_prev():
    s = make_storage(ttl_seconds=0.2)
    ctx = default_context()
    await s.start(ctx)

    loop = asyncio.get_event_loop()
    s._current["key"] = Item[str](loop.time() + 3600.0, 1)

    # Wait for first rotation (high_water_mark=0 → always rotates)
    await asyncio.sleep(0.3)

    # After rotation: prev = old_current, current = {}
    assert "key" in s._prev
    assert "key" not in s._current

    await s.stop(ctx)


@pytest.mark.asyncio
async def test_rotation_preserves_prev_items():
    # RotatingMap copies prev into current on rotate — items in prev survive.
    s = make_storage(ttl_seconds=0.05)
    ctx = default_context()
    await s.start(ctx)

    loop = asyncio.get_event_loop()
    s._prev["persistent"] = Item[str](loop.time() + 3600.0, 1)

    await asyncio.sleep(0.15)  # one rotation fires

    # Item must still be accessible (in current or prev)
    assert "persistent" in s._current or "persistent" in s._prev

    await s.stop(ctx)


@pytest.mark.asyncio
async def test_rotation_never_evicts_live_items():
    # Unlike the old drop-storage2 approach, RotatingMap rotation never kills live items.
    s = make_storage(ttl_seconds=0.05)
    ctx = default_context()
    await s.start(ctx)

    loop = asyncio.get_event_loop()
    s._current["item"] = Item[str](loop.time() + 3600.0, 1)

    await asyncio.sleep(0.25)  # multiple rotation intervals

    assert "item" in s._current or "item" in s._prev

    await s.stop(ctx)



# ---------- after_func (context.AfterFunc equivalent) ----------

@pytest.mark.asyncio
async def test_after_func_fires_at_deadline_and_removes_item():
    s = make_storage(ttl_seconds=3600)
    called = False

    async def cb(values):
        nonlocal called
        called = True
        return False

    token = request_deadline.set(datetime.now() + timedelta(milliseconds=60))
    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    # Item exists immediately after join_value returns (callback returned False)
    assert "k1" in s._current

    # Wait for after_func to fire at deadline
    await asyncio.sleep(0.15)

    # after_func must have fired and removed the item
    assert "k1" not in s._current
    assert "k1" not in s._prev
    assert called  # callback was invoked by after_func


@pytest.mark.asyncio
async def test_after_func_cancelled_when_item_processed():
    s = make_storage(ttl_seconds=3600)
    call_count = 0

    async def cb(values):
        nonlocal call_count
        call_count += 1
        return True  # processed immediately

    token = request_deadline.set(datetime.now() + timedelta(milliseconds=60))
    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    await asyncio.sleep(0.15)

    # Callback called exactly once (by join_value, after_func was cancelled)
    assert call_count == 1


@pytest.mark.asyncio
async def test_after_func_not_created_for_infinite_deadline():
    # Expired context deadline → ttl_seconds=0 → deadline=float('inf') → no after_task
    # (item has no individual expiry, cleaned up lazily or by explicit callback=True).
    s = make_storage(ttl_seconds=3600)
    token = request_deadline.set(datetime.now() - timedelta(seconds=1))

    async def cb(values):
        return False

    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    item = s._current.get("k1")
    assert item is not None
    assert item.deadline == float('inf')
    assert item.after_task is None


@pytest.mark.asyncio
async def test_after_func_renew_ttl_restarts_timer():
    s = make_storage(ttl_seconds=3600, renew_ttl=True)
    call_count = 0

    async def cb(values):
        nonlocal call_count
        call_count += 1
        return False

    loop = asyncio.get_event_loop()
    token = request_deadline.set(datetime.now() + timedelta(milliseconds=80))
    try:
        await s.join_value("k1", 0, "v1", cb)
        first_task = s._current["k1"].after_task

        # Renew TTL: old after_task cancelled, new one created
        request_deadline.reset(token)
        token = request_deadline.set(datetime.now() + timedelta(milliseconds=80))
        await s.join_value("k1", 0, "v2", cb)
        second_task = s._current["k1"].after_task
    finally:
        request_deadline.reset(token)

    assert first_task is not second_task
    await asyncio.sleep(0)  # yield so event loop processes the cancellation
    assert first_task is not None
    assert first_task.cancelled()


# ---------- deadline override via ContextVar (mirrors Go's ctx.Deadline()) ----------

@pytest.mark.asyncio
async def test_join_value_request_deadline_overrides_config_ttl():
    s = make_storage(ttl_seconds=3600)
    loop = asyncio.get_event_loop()
    token = request_deadline.set(datetime.now() + timedelta(milliseconds=50))

    async def cb(values):
        return False

    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    item = s._current.get("k1")
    assert item is not None
    # Item deadline ≈ loop.time() + 50ms; must be well under 1 hour from now
    assert item.deadline <= loop.time() + 0.1  # 50ms + generous tolerance


@pytest.mark.asyncio
async def test_join_value_expired_request_deadline_item_has_zero_ttl():
    # Past deadline → ttl = 0 → item stored with float('inf') deadline
    # (mirrors Go's zero time.Time — join semantics preserved even for expired contexts).
    s = make_storage(ttl_seconds=3600)
    token = request_deadline.set(datetime.now() - timedelta(seconds=1))
    called = False

    async def cb(values):
        nonlocal called
        called = True
        return False  # keep item to verify it's stored

    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    assert called
    item = s._current.get("k1")
    assert item is not None
    assert item.deadline == float('inf')


@pytest.mark.asyncio
async def test_join_value_no_request_deadline_uses_config_ttl():
    s = make_storage(ttl_seconds=10)
    loop = asyncio.get_event_loop()
    before_ts = loop.time()

    async def cb(values):
        return False

    await s.join_value("k1", 0, "v", cb)

    item = s._current.get("k1")
    assert item is not None
    assert before_ts + 9.0 <= item.deadline <= before_ts + 11.0


@pytest.mark.asyncio
async def test_join_value_renew_ttl_uses_request_deadline_derived_ttl():
    s = make_storage(ttl_seconds=3600, renew_ttl=True)
    loop = asyncio.get_event_loop()
    token = request_deadline.set(datetime.now() + timedelta(milliseconds=100))

    async def cb(values):
        return False

    try:
        await s.join_value("k1", 0, "v", cb)
    finally:
        request_deadline.reset(token)

    item = s._current.get("k1")
    assert item is not None
    assert item.deadline < loop.time() + 1.0  # 100ms deadline, not 3600s config TTL


@pytest.mark.asyncio
async def test_request_deadline_propagated_to_nested_coroutine():
    s = make_storage(ttl_seconds=3600)
    loop = asyncio.get_event_loop()
    token = request_deadline.set(datetime.now() + timedelta(milliseconds=50))

    async def nested():
        async def cb(values):
            return False
        await s.join_value("k1", 0, "v", cb)

    try:
        await nested()
    finally:
        request_deadline.reset(token)

    item = s._current.get("k1")
    assert item is not None
    assert item.deadline <= loop.time() + 0.1


# ---------- JoinStorageFactory ----------

def test_join_storage_factory_creates_hashmap():
    from pyservicelib_gorundebug.api.models.join_storage_type import JoinStorageType
    cfg = MockJoinStorageConfig(ttl=timedelta(hours=1))
    storage: HashMapJoinStorage = JoinStorageFactory.make_storage(JoinStorageType.HashMap, env=_ENV, cfg=cfg)  # type: ignore[arg-type,assignment]
    assert isinstance(storage, HashMapJoinStorage)


def test_join_storage_factory_invalid_type_raises():
    cfg = MockJoinStorageConfig(ttl=timedelta(hours=1))
    with pytest.raises(ValueError):
        JoinStorageFactory.make_storage("InvalidType", env=None, cfg=cfg)  # type: ignore

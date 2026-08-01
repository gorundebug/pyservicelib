#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import pytest

from pyservicelib_gorundebug.runtime.store.rotatingmap import RotatingMap, _SHRINK_FACTOR
from pyservicelib_gorundebug.runtime.context import default_context


# ---------- construction ----------

def test_initial_state():
    m: RotatingMap[str, int] = RotatingMap(interval=1.0)
    assert m._current == {}
    assert m._prev == {}
    assert m._timer_task is None
    assert m._high_water_mark == 0


# ---------- lifecycle ----------

@pytest.mark.asyncio
async def test_start_creates_timer_task():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    ctx = default_context()
    await m.start(ctx)
    assert m._timer_task is not None
    assert not m._timer_task.done()
    await m.stop(ctx)


@pytest.mark.asyncio
async def test_stop_cancels_timer_task():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    ctx = default_context()
    await m.start(ctx)
    await m.stop(ctx)
    assert m._timer_task is not None
    assert m._timer_task.done()


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    ctx = default_context()
    await m.stop(ctx)  # must not raise


# ---------- set / get ----------

def test_get_existing_key():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 42)
    v, ok = m.get("k")
    assert ok
    assert v == 42


def test_get_missing_key_returns_false():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    v, ok = m.get("missing")
    assert not ok
    assert v is None


def test_get_value_none_is_distinguishable():
    m: RotatingMap[str, None] = RotatingMap(interval=3600)
    m.set("k", None)
    v, ok = m.get("k")
    assert ok        # key found
    assert v is None  # value is None — not ambiguous with "not found"


def test_set_duplicate_in_current_raises():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 1)
    with pytest.raises(ValueError):
        m.set("k", 2)


def test_set_duplicate_in_prev_raises():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 1)
    m._rotate()  # k moves to prev
    with pytest.raises(ValueError):
        m.set("k", 2)


# ---------- get_or_create ----------

def test_get_or_create_creates_when_missing():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    v, loaded = m.get_or_create("k", lambda: 42)
    assert not loaded
    assert v == 42
    assert m._current["k"] == 42


def test_get_or_create_returns_existing_from_current():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 1)
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return 2

    v, loaded = m.get_or_create("k", factory)
    assert loaded
    assert v == 1
    assert calls == 0  # factory must not run when key already exists


def test_get_or_create_returns_existing_from_prev():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 1)
    m._rotate()  # k moves to prev

    v, loaded = m.get_or_create("k", lambda: 2)
    assert loaded
    assert v == 1
    assert "k" not in m._current


# ---------- pop ----------

def test_pop_existing_key():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 7)
    v, ok = m.pop("k")
    assert ok
    assert v == 7
    _, ok = m.get("k")
    assert not ok


def test_pop_missing_key_returns_false():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    v, ok = m.pop("missing")
    assert not ok
    assert v is None


def test_pop_twice_on_same_key():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 5)
    m.pop("k")
    _, ok = m.pop("k")
    assert not ok


def test_pop_value_none_is_distinguishable():
    m: RotatingMap[str, None] = RotatingMap(interval=3600)
    m.set("k", None)
    v, ok = m.pop("k")
    assert ok
    assert v is None
    _, ok = m.pop("k")
    assert not ok


# ---------- rotate ----------

def test_rotate_item_moves_to_prev():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 10)
    m._rotate()

    assert len(m._current) == 0
    assert m._prev.get("k") == 10
    v, ok = m.get("k")
    assert ok and v == 10


def test_rotate_pop_from_prev():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 20)
    m._rotate()

    v, ok = m.pop("k")
    assert ok and v == 20
    _, ok = m.get("k")
    assert not ok


def test_rotate_two_rotations_item_survives():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 5)

    m._rotate()  # current={} prev={k:5}, highWaterMark=1
    # Second rotate: total=1, 1*4=4 >= 1 → skipped (no memory waste)
    m._rotate()

    v, ok = m.get("k")
    assert ok and v == 5


def test_rotate_get_searches_current_then_prev():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("a", 1)
    m._rotate()  # a is in prev

    v, ok = m.get("a")
    assert ok and v == 1

    m.set("b", 2)
    v, ok = m.get("b")
    assert ok and v == 2


def test_rotate_pop_searches_current_then_prev():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("a", 1)
    m._rotate()

    v, ok = m.pop("a")
    assert ok and v == 1
    _, ok = m.get("a")
    assert not ok

    m.set("b", 2)
    v, ok = m.pop("b")
    assert ok and v == 2


def test_rotate_set_after_rotate_goes_into_current():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("a", 1)
    m._rotate()

    m.set("b", 2)
    assert "b" in m._current
    assert "b" not in m._prev


def test_rotate_merge_preserves_items_from_both_generations():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("a", 1)
    m._rotate()  # a in prev; highWaterMark=1

    m.set("b", 2)
    # total=2, 2*4=8 >= 1 → skipped; but items still accessible
    m._rotate()

    v, ok = m.get("a")
    assert ok and v == 1
    v, ok = m.get("b")
    assert ok and v == 2


# ---------- shrink-factor rotation guard ----------

def test_skips_rotation_under_high_load():
    m: RotatingMap[int, int] = RotatingMap(interval=3600)
    peak = 100
    for i in range(peak):
        m.set(i, i)
    m._rotate()  # first rotation always fires; highWaterMark=peak; all in prev

    len_prev_before = len(m._prev)
    len_current_before = len(m._current)

    # total=100, 100*4=400 >= 100 → must skip
    m._rotate()

    assert len(m._prev) == len_prev_before
    assert len(m._current) == len_current_before
    v, ok = m.get(0)
    assert ok and v == 0  # items still accessible


def test_rotates_after_burst_recovery():
    m: RotatingMap[int, int] = RotatingMap(interval=3600)
    peak = 100
    for i in range(peak):
        m.set(i, i)
    m._rotate()  # highWaterMark=peak; all in prev

    # Pop enough to drop below peak/_SHRINK_FACTOR
    threshold = peak // _SHRINK_FACTOR  # = 25
    for i in range(peak - threshold + 1):
        m.pop(i)
    # live = threshold-1 = 24 < 25 → rotation must fire

    m._rotate()

    assert len(m._current) == 0  # fresh empty current created
    assert m._high_water_mark == threshold - 1  # reset to current live count


def test_high_water_mark_tracked_when_skipped():
    m: RotatingMap[int, int] = RotatingMap(interval=3600)

    # First rotation with 10 entries
    for i in range(10):
        m.set(i, i)
    m._rotate()  # highWaterMark=10

    # Add 200 more (growing load), trigger a skipped rotation
    for i in range(10, 210):
        m.set(i, i)
    m._rotate()  # total~210, 210*4 >= 10 → skip; but highWaterMark must update

    assert m._high_water_mark >= 200


def test_first_call_always_rotates():
    m: RotatingMap[str, int] = RotatingMap(interval=3600)
    m.set("k", 1)

    m._rotate()  # highWaterMark was 0 → must rotate

    assert len(m._current) == 0
    assert m._prev.get("k") == 1


# ---------- timer-based rotation ----------

@pytest.mark.asyncio
async def test_timer_triggers_rotation():
    m: RotatingMap[str, int] = RotatingMap(interval=0.1)
    m.set("k", 99)

    ctx = default_context()
    await m.start(ctx)
    await asyncio.sleep(0.25)  # wait for at least one rotation
    await m.stop(ctx)

    # After rotation: item moved to prev, still accessible
    v, ok = m.get("k")
    assert ok and v == 99
    assert "k" not in m._current  # moved to prev on first rotate

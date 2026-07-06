#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Optional

from ..context import Context

# Rotate only when live entries have dropped below 1/SHRINK_FACTOR of the peak.
# Mirrors the Go constant rotatingMapShrinkFactor = 4.
_SHRINK_FACTOR = 4


class RotatingMap[K, V]:
    """Two-bucket map for memory reclamation after burst traffic.

    Items survive rotation (prev is merged into current on each rotation).
    The rotation timer fires periodically but the actual swap happens only when
    live entries < highWaterMark / _SHRINK_FACTOR to avoid pointless copies.
    """

    def __init__(self, interval: float):
        """
        Args:
            interval: rotation check interval in seconds.
        """
        self._interval = interval
        self._current: dict[K, V] = {}
        self._prev: dict[K, V] = {}
        self._timer_task: Optional[asyncio.Task] = None
        self._high_water_mark: int = 0

    async def start(self, ctx: Context) -> None:
        self._timer_task = asyncio.create_task(self._rotation_loop())

    async def stop(self, ctx: Context) -> None:
        if self._timer_task is not None:
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass

    async def _rotation_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval)
                self._rotate()
        except asyncio.CancelledError:
            pass

    def _rotate(self) -> None:
        total = len(self._current) + len(self._prev)

        # Compute before updating _high_water_mark so first call (= 0) always rotates.
        should_rotate = self._high_water_mark == 0 or total * _SHRINK_FACTOR < self._high_water_mark

        if total > self._high_water_mark:
            self._high_water_mark = total

        if not should_rotate:
            return

        # Reset water mark so the next burst is measured from a clean baseline.
        self._high_water_mark = total

        # Merge prev into current, then swap: current becomes prev, fresh map becomes current.
        for k, v in self._prev.items():
            self._current[k] = v
        self._prev = self._current
        self._current = {}

    def set(self, key: K, value: V) -> None:
        """Insert key. Raises ValueError on duplicate (key exists in either bucket)."""
        if key in self._current:
            raise ValueError(f"duplicate key {key!r}")
        if key in self._prev:
            raise ValueError(f"duplicate key {key!r}")
        self._current[key] = value

    def get(self, key: K) -> tuple[V | None, bool]:
        """Return (value, True) if found, (None, False) otherwise. Mirrors Go's (V, bool)."""
        if key in self._current:
            return self._current[key], True
        if key in self._prev:
            return self._prev[key], True
        return None, False

    def pop(self, key: K) -> tuple[V | None, bool]:
        """Remove and return (value, True) if found, (None, False) otherwise."""
        if key in self._current:
            return self._current.pop(key), True
        if key in self._prev:
            return self._prev.pop(key), True
        return None, False

import subprocess
import sys

import pytest


CHILD = r'''
import asyncio
import sys
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from pyservicelib_gorundebug.runtime.context import request_cancelled
from pyservicelib_gorundebug.runtime.pool import PoolCancelledError
from pyservicelib_gorundebug.runtime.store.hashmap import HashMapJoinStorage

async def main():
    env = MagicMock()
    env.service_config.name = "expiry-policy"
    env.metrics.enabled = False
    storage = HashMapJoinStorage(env, SimpleNamespace(
        name="join", ttl=timedelta(milliseconds=10), renew_ttl=False,
    ))
    cancelled = asyncio.Event()
    request_cancelled.set(cancelled)
    calls = 0

    async def callback(values):
        nonlocal calls
        calls += 1
        if calls == 1:
            return False
        print("EXPIRY_ENTERED", flush=True)
        kind = sys.argv[1]
        if kind == "cancel":
            raise asyncio.CancelledError()
        if kind == "pool_cancel":
            raise PoolCancelledError()
        if kind == "error_after_cancel":
            cancelled.set()
        raise RuntimeError("expiry callback failed")

    await storage.join_value("key", 0, "value", callback)
    await asyncio.sleep(0.1)
    assert not storage._current and not storage._prev
    assert not storage._after_tasks
    print("PROCESS_SURVIVED", flush=True)

asyncio.run(main())
'''


@pytest.mark.parametrize("kind", ["error", "error_after_cancel", "cancel", "pool_cancel"])
def test_join_expiry_background_failure_policy(kind: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", CHILD, kind],
        capture_output=True, text=True, timeout=15,
    )
    assert "EXPIRY_ENTERED" in result.stdout, result.stderr
    if kind in ("error", "error_after_cancel"):
        assert result.returncode == 2, (result.stdout, result.stderr)
        assert "expiry callback failed" in result.stderr
        assert "PROCESS_SURVIVED" not in result.stdout
    else:
        assert result.returncode == 0, result.stderr
        assert "PROCESS_SURVIVED" in result.stdout
        assert "Task exception was never retrieved" not in result.stderr

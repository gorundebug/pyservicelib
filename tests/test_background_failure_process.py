"""Unhandled detached failures must terminate, not just reach an asyncio log."""

import subprocess
import sys

import pytest


CHILD = r'''
import asyncio
import sys
from datetime import timedelta
from unittest.mock import MagicMock

from pyservicelib_gorundebug.runtime.context import default_context
from pyservicelib_gorundebug.runtime.context.request import request_cancelled
from pyservicelib_gorundebug.runtime.pool import PoolCancelledError
from pyservicelib_gorundebug.runtime.pool.taskpool import TaskPoolImpl
from pyservicelib_gorundebug.runtime.pool.prioritytaskpool import PriorityTaskPoolImpl
from pyservicelib_gorundebug.runtime.pool.delaypool import DelayPoolImpl
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp


async def main():
    kind, behavior = sys.argv[1:]
    env = MagicMock()
    env.config.get_pool_by_name.return_value.executors_count = 1
    env.metrics.enabled = False
    env.log.warn.side_effect = RuntimeError("broken logger")
    cancel = asyncio.Event()
    token = request_cancelled.set(cancel)

    async def callback():
        print("CALLBACK_ENTERED", flush=True)
        if behavior == "cancel":
            raise asyncio.CancelledError()
        if behavior == "pool_cancel":
            raise PoolCancelledError()
        if behavior == "error_after_cancel":
            cancel.set()
        raise RuntimeError("unhandled business callback")

    pool = None
    if kind == "parallel":
        app = ServiceApp()
        app.create_task(callback)
    else:
        if kind == "task":
            pool = TaskPoolImpl("p", env)
        elif kind == "priority":
            pool = PriorityTaskPoolImpl("p", env)
        else:
            pool = DelayPoolImpl(env)
        await pool.start(default_context())
        if kind == "priority":
            await pool.add_task(0, callback)
        elif kind == "delay":
            await pool.add_task(timedelta(0), callback)
        else:
            await pool.add_task(callback)
    await asyncio.sleep(0.1)
    if pool is not None:
        await pool.stop(default_context())
    request_cancelled.reset(token)
    print("PROCESS_SURVIVED", flush=True)


asyncio.run(main())
'''


@pytest.mark.parametrize("kind", ["task", "priority", "delay", "parallel"])
@pytest.mark.parametrize("behavior", ["error", "error_after_cancel", "cancel", "pool_cancel"])
def test_background_callback_process_policy(kind: str, behavior: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", CHILD, kind, behavior],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    diagnostic = f"stdout={result.stdout!r}, stderr={result.stderr!r}"
    assert "CALLBACK_ENTERED" in result.stdout, diagnostic
    if behavior in ("cancel", "pool_cancel"):
        assert result.returncode == 0, diagnostic
        assert "PROCESS_SURVIVED" in result.stdout, diagnostic
        assert "Task exception was never retrieved" not in result.stderr, diagnostic
    else:
        assert result.returncode == 2, diagnostic
        assert "PROCESS_SURVIVED" not in result.stdout, diagnostic
        assert "unhandled business callback" in result.stderr, diagnostic

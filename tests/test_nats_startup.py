import asyncio
import os
import time
from datetime import timedelta

import pytest
from pyservicelib_gorundebug.runtime.config import NatsDataConnectorConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.nats import (
    Connection,
    TransportStopped,
    wait_context,
)


@pytest.mark.asyncio
async def test_cancel_at_completion_boundary():
    owner = Context()

    async def operation():
        owner.cancel()
        return "late-success"

    with pytest.raises(TransportStopped):
        await wait_context(operation(), owner)


@pytest.mark.asyncio
async def test_native_startup_gate():
    url = os.environ.get("NATS_STARTUP_URL")
    if not url:
        pytest.skip("native startup proxy required")
    mode = os.environ["NATS_STARTUP_MODE"]
    owner = Context(timedelta(milliseconds=100) if mode == "deadline" else None)
    if mode == "pre-cancel":
        owner.cancel()
    timer = (
        asyncio.get_running_loop().call_later(0.04, owner.cancel)
        if mode == "cancel"
        else None
    )
    cfg = NatsDataConnectorConfig(
        id=1,
        name="startup",
        servers=[url],
        connectTimeoutMs=100 if mode == "configured-timeout" else 1200,
    )

    async def report(_error):
        pass

    connection = Connection(cfg, report)
    begin = time.monotonic()
    failed = False
    try:
        try:
            await connection.start(owner)
        except (TransportStopped, TimeoutError):
            failed = True
        assert failed == (mode != "control")
        if failed:
            assert not connection.ready.is_set()
        assert time.monotonic() - begin < 0.7
    finally:
        if timer:
            timer.cancel()
        await connection.close()

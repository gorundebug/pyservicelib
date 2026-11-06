import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pyservicelib_gorundebug.runtime.config import CustomEndpointConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.datasource.localsource.custom import TypedCustomEndpointConsumer

@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_custom_source_enabled_controls_producer_lifecycle(enabled):
    config = CustomEndpointConfig(1, "source", 2, enabled=enabled)
    assert config.to_dict()["enabled"] is enabled
    # Exercise the actual endpoint-consumer lifecycle, without constructing an unrelated graph.
    consumer = object.__new__(TypedCustomEndpointConsumer)
    consumer._endpoint = SimpleNamespace(config=config)
    consumer._data_producer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    consumer._has_result = False
    consumer._pending = None
    consumer._runner_task = None
    consumer._tasks = set()
    ctx = Context()
    await consumer.start(ctx)
    await asyncio.sleep(0)
    await consumer.stop(ctx)
    assert consumer._data_producer.start.await_count == int(enabled)
    assert consumer._data_producer.stop.await_count == int(enabled)
    if not enabled:
        assert consumer._runner_task is None
        assert consumer._pending is None

def test_custom_endpoint_default_is_disabled():
    assert not CustomEndpointConfig(1, "source", 2).enabled

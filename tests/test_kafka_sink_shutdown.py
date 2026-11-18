"""Stop owns the native close; cancellation limits only the caller's wait."""
import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.datasink.kafka.aiokafkads import _AIOKafkaSinkDataSink
from pyservicelib_gorundebug.runtime.context import Context


def sink():
    value = object.__new__(_AIOKafkaSinkDataSink)
    value._stopped = False
    value._shutdown_task = None
    value._endpoints = {1: SimpleNamespace(stop=AsyncMock())}
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel_mode', ['deadline', 'context', 'task'])
async def test_stop_keeps_one_native_close_and_late_delivery_owned(cancel_mode):
    ds = sink()
    entered, release = asyncio.Event(), asyncio.Event()
    delivered = []
    future = asyncio.get_running_loop().create_future()

    async def close():
        entered.set()
        await release.wait()
        future.set_result(SimpleNamespace(partition=0, offset=9))
    ds._producer = SimpleNamespace(stop=AsyncMock(side_effect=close),
                                   send=AsyncMock(return_value=future))
    publication = asyncio.create_task(ds.send_message('events', None, b'value', None, None,
        lambda p, o, e: delivered.append((p, o, e))))
    await asyncio.sleep(0)
    owner = Context(timeout=timedelta(milliseconds=10)) if cancel_mode == 'deadline' else Context()
    first = asyncio.create_task(ds.stop(owner))
    await entered.wait()
    if cancel_mode == 'context': owner.cancel()
    if cancel_mode == 'task': first.cancel()
    with pytest.raises(TimeoutError if cancel_mode == 'deadline' else asyncio.CancelledError):
        await first
    assert not ds._shutdown_task.done()
    assert not publication.done()
    assert delivered == []
    rejected = []
    await ds.send_message('events', None, b'late', None, None, lambda *r: rejected.append(r))
    assert isinstance(rejected[0][2], RuntimeError)
    with pytest.raises(RuntimeError, match='stopped'):
        await ds.start(Context())
    second = asyncio.create_task(ds.stop(Context()))
    await asyncio.sleep(0)
    assert not second.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(second, publication), 1)
    await ds.stop(Context())
    ds._producer.stop.assert_awaited_once()
    ds._endpoints[1].stop.assert_awaited_once()
    assert delivered == [(0, 9, None)]


@pytest.mark.asyncio
async def test_native_close_failure_is_not_success_on_repeat_stop():
    ds = sink()
    failure = RuntimeError('native close failed')
    ds._producer = SimpleNamespace(stop=AsyncMock(side_effect=failure))
    for _ in range(2):
        with pytest.raises(RuntimeError, match='native close failed'):
            await ds.stop(Context())
    ds._producer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_unstarted_sink_stops_endpoints_without_a_producer():
    ds = sink()
    ds._producer = None
    await ds.stop(Context())
    await ds.stop(Context())
    ds._endpoints[1].stop.assert_awaited_once()

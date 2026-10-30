"""Public factories route delivery through explicit graph contracts."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.datasink.kafka.aiokafkads import (
    make_aiokafka_endpoint_consumer,
    make_aiokafka_endpoint_consumer_with_result,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import with_stream_id, stream_id_from_context


class Handler:
    def __init__(self, mode):
        self.mode = mode
        self.ended = []

    def get_stream_id(self, value):
        return 'message-key'

    def begin_request(self, stream):
        return None

    async def consume_message(self, stream, state, value, message):
        if self.mode == 'skip':
            await message.skip(value)
        elif self.mode == 'sync':
            result = await message.send_sync()
            await message.out(result)
        elif self.mode == 'error':
            raise ValueError('handler failed')
        else:
            message.send(lambda partition, offset, error: (partition, offset, error))

    async def end_request(self, stream, error, state):
        self.ended.append(error)


def fixture():
    tasks, deliveries, registered = [], [], []

    def schedule(fn, *args):
        tasks.append(asyncio.create_task(fn(*args)))

    async def send(topic, key, value, partition, headers, callback):
        deliveries.append(callback)

    endpoint = SimpleNamespace(name='publish', enabled=True, topic='events',
        add_endpoint_consumer=registered.append, on_request_start=lambda: 1.0,
        on_request_end=lambda start, error: None)
    ds = SimpleNamespace(get_endpoint=lambda id: endpoint,
                         partitions_for=AsyncMock(return_value=1), send_message=send)
    env = SimpleNamespace(tracing=None, runtime=SimpleNamespace(create_task=schedule),
        config=SimpleNamespace(get_endpoint_config_by_id=lambda id: SimpleNamespace(id_data_connector=1)),
        get_datasink=lambda id: ds)
    endpoint.environment, endpoint.datasink = env, ds

    def stream():
        result, error = AsyncMock(), AsyncMock()
        obj = SimpleNamespace(environment=env, endpoint_id=100, name='publish',
            config=SimpleNamespace(pipeline='', component=''),
            error_stream=SimpleNamespace(consume=error), consume_result=result,
            set_sink_consumer=lambda consumer: None)
        return obj, result, error

    async def drain():
        while tasks:
            batch = tasks[:]
            tasks.clear()
            await asyncio.gather(*batch)

    return stream, deliveries, registered, drain


@pytest.mark.parametrize('with_result', [False, True])
@pytest.mark.parametrize('mode', ['skip', 'async', 'sync', 'error'])
@pytest.mark.asyncio
async def test_explicit_result_route(with_result, mode):
    make_stream, deliveries, registered, drain = fixture()
    stream, result, error = make_stream()
    handler = Handler(mode)
    factory = make_aiokafka_endpoint_consumer_with_result if with_result else make_aiokafka_endpoint_consumer
    consumer = factory(stream, handler)
    assert registered == [consumer]
    await consumer.start(Context())
    with_stream_id('request-owned')
    pending = asyncio.create_task(consumer.consume('value'))
    await asyncio.sleep(0)
    await drain()
    target, other = (result, error) if with_result else (error, result)
    if mode in ('async', 'sync'):
        target.assert_not_awaited()
        assert len(deliveries) == 1
        deliveries.pop()(0, 17, None)
        await drain()
    await pending
    other.assert_not_awaited()
    if mode == 'error':
        target.assert_not_awaited()
        assert isinstance(handler.ended[0], ValueError)
    else:
        target.assert_awaited_once_with('value' if mode == 'skip' else (0, 17, None))
        assert handler.ended == [None]
    assert stream_id_from_context() == 'request-owned'


@pytest.mark.asyncio
async def test_shared_endpoint_keeps_consumer_result_routes_separate():
    make_stream, deliveries, registered, drain = fixture()
    plain, plain_result, plain_error = make_stream()
    producing, produced, producing_error = make_stream()
    first = make_aiokafka_endpoint_consumer(plain, Handler('async'))
    second = make_aiokafka_endpoint_consumer_with_result(producing, Handler('async'))
    assert len(registered) == 2
    await first.start(Context())
    await second.start(Context())
    await first.consume('a')
    await second.consume('b')
    await drain()
    failure = RuntimeError('broker rejected')
    deliveries[1](1, 21, failure)
    deliveries[0](0, 11, None)
    await drain()
    plain_error.assert_awaited_once_with((0, 11, None))
    produced.assert_awaited_once_with((1, 21, failure))
    plain_result.assert_not_awaited()
    producing_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_sync_wait_does_not_publish_a_late_result():
    make_stream, deliveries, registered, drain = fixture()
    stream, result, error = make_stream()
    consumer = make_aiokafka_endpoint_consumer_with_result(stream, Handler('sync'))
    await consumer.start(Context())
    pending = asyncio.create_task(consumer.consume('value'))
    await asyncio.sleep(0)
    await drain()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    deliveries[0](0, 17, None)
    await drain()
    result.assert_not_awaited()
    error.assert_not_awaited()

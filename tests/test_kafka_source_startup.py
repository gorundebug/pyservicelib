import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pyservicelib_gorundebug.datasource.kafka import aiokafkads as kafka
from pyservicelib_gorundebug.runtime.context import Context
from tests.test_kafka_source_lifecycle import consumer


def prepare_consumer(instance):
    instance._kafka_consumer = None
    config = SimpleNamespace(enabled=True, id_data_connector=1, topic='events', consumer_group='group')
    instance._endpoint = SimpleNamespace(config=config, name='events', environment=SimpleNamespace(
        config=SimpleNamespace(get_data_connector_by_id=lambda id: SimpleNamespace(brokers='fake:9092')),
    ))


def source(endpoints=()):
    env = SimpleNamespace(config=SimpleNamespace(get_data_connector_by_id=lambda id:
        SimpleNamespace(name='source', brokers='fake:9092')))
    result = kafka._AIOKafkaDataSource(1, env)
    result._endpoints = dict(enumerate(endpoints))
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['caller', 'context', 'deadline', 'stop'])
async def test_partial_connect_remains_owned_until_connect_exits(monkeypatch, mode):
    instance = consumer()
    prepare_consumer(instance)
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def connect():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
            raise

    transport = SimpleNamespace(start=connect, stop=AsyncMock())
    def build_consumer(*args, **kwargs):
        assert kwargs['isolation_level'] == 'read_committed'
        return transport

    monkeypatch.setattr(kafka, 'AIOKafkaConsumer', build_consumer)
    parent = Context(timeout=timedelta(milliseconds=25)) if mode == 'deadline' else Context()
    starting = asyncio.create_task(instance.start(parent))
    await entered.wait()
    assert instance._kafka_consumer is transport
    if mode == 'caller':
        starting.cancel()
    elif mode == 'context':
        parent.cancel()
    elif mode == 'stop':
        short = Context(timeout=timedelta(milliseconds=5))
        with pytest.raises(TimeoutError):
            await instance.stop(short)
    expected = TimeoutError if mode == 'deadline' else asyncio.CancelledError
    if mode != 'stop':
        with pytest.raises(expected):
            await starting
    await asyncio.wait_for(cancelled.wait(), 1)
    transport.stop.assert_not_awaited()
    assert not instance._start_task.done()
    with pytest.raises(RuntimeError, match='stopped|draining'):
        await instance.start(Context())
    release.set()
    await asyncio.wait_for(instance.stop(Context()), 1)
    if mode == 'stop':
        with pytest.raises(asyncio.CancelledError):
            await starting
    transport.stop.assert_awaited_once()
    assert instance._kafka_consumer is None
    assert instance._runner_task is None
    assert instance._auto_commit_task is None


@pytest.mark.asyncio
async def test_failed_connect_preserves_primary_error_and_owns_cleanup_failure(monkeypatch):
    instance = consumer()
    prepare_consumer(instance)
    primary = ValueError('connect failed')
    secondary = RuntimeError('disconnect failed')
    transport = SimpleNamespace(start=AsyncMock(side_effect=primary), stop=AsyncMock(side_effect=secondary))
    monkeypatch.setattr(kafka, 'AIOKafkaConsumer', lambda *a, **kw: transport)
    with pytest.raises(ValueError) as caught:
        await instance.start(Context())
    assert caught.value is primary
    with pytest.raises(RuntimeError) as cleanup:
        await instance.stop(Context())
    assert cleanup.value is secondary
    assert instance._kafka_consumer is transport
    with pytest.raises(RuntimeError) as retry:
        await instance.stop(Context())
    assert retry.value is secondary
    transport.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_datasource_rolls_back_partial_endpoint_and_retains_failures():
    cfg = SimpleNamespace(enabled=True, create_topic=False)
    first = SimpleNamespace(config=cfg, start=AsyncMock(), stop=AsyncMock())
    primary = ValueError('second start failed')
    secondary = RuntimeError('second cleanup failed')
    second = SimpleNamespace(config=cfg, start=AsyncMock(side_effect=primary),
                             stop=AsyncMock(side_effect=[secondary, None]))
    third = SimpleNamespace(config=cfg, start=AsyncMock(), stop=AsyncMock())
    ds = source([first, second, third])
    with pytest.raises(ValueError) as caught:
        await ds.start(Context())
    assert caught.value is primary
    await asyncio.gather(ds._shutdown_task, return_exceptions=True)
    assert ds._active_endpoints == [second]
    first.stop.assert_awaited_once()
    third.start.assert_not_awaited()
    third.stop.assert_not_awaited()
    await ds.stop(Context())
    first.stop.assert_awaited_once()
    assert second.stop.await_count == 2
    assert not ds._active_endpoints


@pytest.mark.asyncio
async def test_datasource_caller_cancellation_retains_start_and_rollback():
    entered, release = asyncio.Event(), asyncio.Event()
    async def begin(ctx):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
            raise
    endpoint = SimpleNamespace(config=SimpleNamespace(enabled=True, create_topic=False),
                               start=begin, stop=AsyncMock())
    ds = source([endpoint])
    pending = asyncio.create_task(ds.start(Context()))
    await entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert ds._active_endpoints == [endpoint]
    with pytest.raises(TimeoutError):
        await ds.stop(Context(timeout=timedelta(milliseconds=5)))
    endpoint.stop.assert_not_awaited()
    release.set()
    await asyncio.wait_for(ds.stop(Context()), 1)
    endpoint.stop.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('close_fails', [False, True])
async def test_admin_partial_start_cleanup_is_owned_and_preserves_primary(monkeypatch, close_fails):
    primary = ValueError('admin bootstrap failed')
    secondary = RuntimeError('admin close failed')
    admin = SimpleNamespace(start=AsyncMock(side_effect=primary), create_topics=AsyncMock(),
        close=AsyncMock(side_effect=secondary if close_fails else None))
    monkeypatch.setattr(kafka, 'AIOKafkaAdminClient', lambda **kw: admin)
    endpoint = SimpleNamespace(config=SimpleNamespace(enabled=True, create_topic=True, topic='events'),
                               start=AsyncMock(), stop=AsyncMock())
    ds = source([endpoint])
    with pytest.raises(ValueError) as caught:
        await ds.start(Context())
    assert caught.value is primary
    if close_fails:
        with pytest.raises(RuntimeError) as cleanup:
            await ds.stop(Context())
        assert cleanup.value is secondary
        assert ds._admin_client is admin
    else:
        await ds.stop(Context())
        assert ds._admin_client is None
    admin.close.assert_awaited_once()
    endpoint.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_admin_close_is_not_cancelled_when_start_caller_leaves(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    async def close():
        entered.set()
        await release.wait()
    admin = SimpleNamespace(start=AsyncMock(), create_topics=AsyncMock(
        return_value=SimpleNamespace(topic_errors=[('events', 0, None)])), close=close)
    monkeypatch.setattr(kafka, 'AIOKafkaAdminClient', lambda **kw: admin)
    endpoint = SimpleNamespace(config=SimpleNamespace(enabled=True, create_topic=True, topic='events'),
                               start=AsyncMock(), stop=AsyncMock())
    ds = source([endpoint])
    pending = asyncio.create_task(ds.start(Context()))
    await entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    with pytest.raises(TimeoutError):
        await ds.stop(Context(timeout=timedelta(milliseconds=5)))
    assert not ds._admin_close_task.done()
    release.set()
    await asyncio.wait_for(ds.stop(Context()), 1)
    assert ds._admin_client is None
    endpoint.start.assert_not_awaited()

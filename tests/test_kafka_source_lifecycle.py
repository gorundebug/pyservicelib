import asyncio
from datetime import timedelta, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pyservicelib_gorundebug.datasource.kafka import aiokafkads as kafka
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled, request_deadline, request_context_scope,
)
from pyservicelib_gorundebug.runtime.store.rotatingmap import RotatingMap


def consumer():
    instance = object.__new__(kafka._AIOKafkaTypedEndpointConsumer)
    instance._owner = Context().child()
    instance._shutdown_task = None
    instance._start_task = None
    instance._transport_close_task = None
    instance._message_errors = []
    instance._stopped = False
    instance._active_count = 0
    instance._concurrency_changed = asyncio.Condition()
    instance._message_tasks = set()
    instance._partition_queues = {}
    instance._paused_partitions = set()
    instance._partition_prefetch = 1
    instance._marked_offsets = {}
    instance._pending = None
    instance._runner_task = None
    instance._auto_commit_task = None
    instance._has_result = False
    instance._sc = object()
    instance._handler = Mock(concurrency=lambda sc: 1)
    instance._kafka_consumer = SimpleNamespace(stop=AsyncMock(), commit=AsyncMock())
    return instance


@pytest.mark.asyncio
async def test_transport_error_without_message_uses_owner_and_no_request_lifecycle():
    instance = consumer()
    failure = RuntimeError('broker unavailable')
    observed = []
    async def on_error(sc, err):
        observed.append((sc, err, request_cancelled.get()))
    instance._handler.on_error = AsyncMock(side_effect=on_error)
    ambient = Context()
    ambient.cancel()
    with request_context_scope(ambient):
        await instance._report_transport_error(failure)
    assert len(observed) == 1
    assert observed[0][0] is instance._sc
    assert observed[0][1] is failure
    assert not observed[0][2].is_set()
    instance._handler.begin_request.assert_not_called()
    instance._handler.consume_message.assert_not_called()
    instance._handler.end_request.assert_not_called()
    instance._owner.cancel()
    await instance._report_transport_error(failure)
    instance._handler.on_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_consume_loop_reports_driver_failure_without_message():
    instance = consumer()
    failure = RuntimeError('offset unavailable')
    class BrokenConsumer:
        def __aiter__(self): return self
        async def __anext__(self): raise failure
    instance._kafka_consumer = BrokenConsumer()
    instance._endpoint = SimpleNamespace(name='events')
    instance._input_stream = SimpleNamespace(environment=SimpleNamespace(log=Mock()))
    instance._handler.on_error = AsyncMock()
    await instance._consume_loop()
    instance._handler.on_error.assert_awaited_once_with(instance._sc, failure)
    instance._handler.begin_request.assert_not_called()
    assert not instance._marked_offsets


@pytest.mark.asyncio
async def test_error_callback_failure_does_not_change_transport_policy():
    instance = consumer()
    instance._endpoint = SimpleNamespace(name='events')
    instance._input_stream = SimpleNamespace(environment=SimpleNamespace(log=Mock()))
    instance._handler.on_error = AsyncMock(side_effect=ValueError('mapping failed'))
    await instance._report_transport_error(RuntimeError('broker unavailable'))
    assert not instance._stopped
    instance._input_stream.environment.log.warn.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['deadline', 'context', 'caller'])
async def test_interrupted_stop_retains_handler_and_transport(mode):
    instance = consumer()
    parent = Context()
    instance._owner = parent.child()
    transport = instance._kafka_consumer
    entered, release = asyncio.Event(), asyncio.Event()
    finished = []

    async def handler():
        entered.set()
        await release.wait()
        finished.append(True)

    task = asyncio.create_task(handler())
    instance._message_tasks.add(task)
    task.add_done_callback(instance._message_finished)
    await entered.wait()
    if mode == 'deadline':
        ctx = Context(timeout=timedelta(milliseconds=5))
        with pytest.raises(TimeoutError):
            await instance.stop(ctx)
    elif mode == 'context':
        ctx = Context()
        ctx.cancel()
        with pytest.raises(asyncio.CancelledError):
            await instance.stop(ctx)
    else:
        waiting = asyncio.create_task(instance.stop(Context()))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    assert not parent.cancelled
    assert instance._owner.cancelled
    assert not task.done()
    assert not instance._shutdown_task.done()
    transport.stop.assert_not_awaited()
    retry_ctx = Context()
    retry_ctx.cancel()
    with pytest.raises(asyncio.CancelledError):
        await instance.stop(retry_ctx)
    release.set()
    await asyncio.wait_for(instance.stop(Context()), 1)
    await instance.stop(Context())
    transport.stop.assert_awaited_once()
    assert finished == [True]
    assert instance._kafka_consumer is None


@pytest.mark.asyncio
async def test_stop_failure_cannot_be_hidden_by_driver_noop_on_retry():
    instance = consumer()
    transport = instance._kafka_consumer
    failure = RuntimeError('disconnect failed')
    transport.stop.side_effect = [failure, None]
    with pytest.raises(RuntimeError) as caught:
        await instance.stop(Context())
    assert caught.value is failure
    assert instance._kafka_consumer is transport
    with pytest.raises(RuntimeError) as again:
        await instance.stop(Context())
    assert again.value is failure
    transport.stop.assert_awaited_once()
    assert instance._kafka_consumer is transport


@pytest.mark.asyncio
async def test_failed_offset_flush_retains_offsets_and_transport():
    instance = consumer()
    transport = instance._kafka_consumer
    transport.commit.side_effect = [RuntimeError('commit failed'), None]
    partition = kafka.TopicPartition('events', 0)
    instance._marked_offsets[partition] = 42
    with pytest.raises(RuntimeError, match='commit failed'):
        await instance.stop(Context())
    transport.stop.assert_not_awaited()
    assert instance._marked_offsets == {partition: 42}
    await instance.stop(Context())
    assert transport.commit.await_count == 2
    assert not instance._marked_offsets
    transport.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_datasource_does_not_swallow_endpoint_errors():
    env = SimpleNamespace(config=SimpleNamespace(
        get_data_connector_by_id=lambda id: SimpleNamespace(name='source'),
    ))
    source = kafka._AIOKafkaDataSource(1, env)
    failure = ValueError('stop failed')
    good = SimpleNamespace(stop=AsyncMock())
    bad = SimpleNamespace(stop=AsyncMock(side_effect=failure))
    source._endpoints = {1: good, 2: bad}
    source._active_endpoints = [good, bad]
    ctx = Context()
    with pytest.raises(ValueError) as caught:
        await source.stop(ctx)
    assert caught.value is failure
    good.stop.assert_awaited_once_with(ctx)
    bad.stop.assert_awaited_once_with(ctx)


@pytest.mark.asyncio
async def test_concurrent_stops_share_one_drain():
    instance = consumer()
    transport = instance._kafka_consumer
    entered, release = asyncio.Event(), asyncio.Event()

    async def disconnect():
        entered.set()
        await release.wait()

    transport.stop.side_effect = disconnect
    first = asyncio.create_task(instance.stop(Context()))
    await entered.wait()
    second = asyncio.create_task(instance.stop(Context()))
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    await second
    transport.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_actual_start_dispatch_binds_owner_instead_of_ambient_context(monkeypatch):
    instance = consumer()
    instance._kafka_consumer = None
    records = asyncio.Queue()
    entered = asyncio.Event()
    observed = []

    class Transport:
        start = AsyncMock()
        stop = AsyncMock()
        commit = AsyncMock()
        pause = Mock()
        resume = Mock()
        def __aiter__(self): return self
        async def __anext__(self): return await records.get()
        def assignment(self): return {kafka.TopicPartition('events', 0)}

    transport = Transport()
    monkeypatch.setattr(kafka, 'AIOKafkaConsumer', lambda *a, **kw: transport)
    config = SimpleNamespace(enabled=True, id_data_connector=1, topic='events', consumer_group='group')
    instance._endpoint = SimpleNamespace(
        config=config, name='events', environment=SimpleNamespace(
            config=SimpleNamespace(get_data_connector_by_id=lambda id: SimpleNamespace(brokers='fake:9092')),
        ),
    )
    async def request(record):
        event = request_cancelled.get()
        observed.append((event, request_deadline.get()))
        entered.set()
        await event.wait()
        observed.append('cancelled')

    instance._endpoint_request = request
    owner = Context(timeout=timedelta(seconds=10))
    ambient = Context()
    ambient.cancel()
    with request_context_scope(ambient):
        await instance.start(owner)
    await records.put(SimpleNamespace(topic='events', partition=0, offset=0))
    await asyncio.wait_for(entered.wait(), 1)
    event, deadline = observed[0]
    assert not event.is_set()
    assert deadline is not None and deadline > datetime.now(timezone.utc)
    await asyncio.wait_for(instance.stop(Context()), 1)
    assert observed[-1] == 'cancelled'
    assert not owner.cancelled
    transport.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_result_wait_finishes_and_removes_pending_on_owner_cancellation():
    instance = consumer()
    instance._has_result = True
    instance._pending = RotatingMap(30)
    instance._tracing = None
    instance._tracer = None
    entered = asyncio.Event()
    errors = []
    instance._input_stream = SimpleNamespace(name='input')
    instance._endpoint = SimpleNamespace(
        name='events', on_request_start=lambda: 1,
        on_request_end=Mock(), on_pending_add=Mock(), on_pending_remove=Mock(),
    )
    async def consume(sc, state, message, result):
        entered.set()
    async def end(sc, err, state):
        errors.append(err)
    instance._handler = SimpleNamespace(
        concurrency=lambda sc: 1, begin_request=AsyncMock(return_value=None),
        consume_message=consume, end_request=end,
    )
    record = SimpleNamespace(topic='events', partition=0, offset=0, key=None, value=b'{}')
    task = asyncio.create_task(instance._dispatch_record(record))
    instance._message_tasks.add(task)
    task.add_done_callback(instance._message_finished)
    await entered.wait()
    await asyncio.wait_for(instance.stop(Context()), 1)
    assert len(errors) == 1 and isinstance(errors[0], RuntimeError)
    instance._endpoint.on_pending_remove.assert_called_once()
    assert instance._active_count == 0

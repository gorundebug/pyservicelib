import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pyservicelib_gorundebug.datasource.kafka import aiokafkads as kafka
from tests.test_kafka_source_lifecycle import consumer


def checkpoint_consumer():
    instance = consumer()
    instance._restoring = True
    instance._endpoint = SimpleNamespace(name='events', config=SimpleNamespace(topic='events'))
    instance._handler.restore_checkpoint = Mock(return_value={0: 12, 1: 18})
    instance._handler.on_error = AsyncMock()
    partitions = {kafka.TopicPartition('events', p) for p in (0, 1)}
    instance._kafka_consumer = SimpleNamespace(
        beginning_offsets=AsyncMock(return_value={p: 10 for p in partitions}),
        end_offsets=AsyncMock(return_value={p: 40 for p in partitions}),
        seek=Mock(), assignment=lambda: partitions, pause=Mock(), resume=Mock(),
        stop=AsyncMock(), commit=AsyncMock(),
    )
    return instance, partitions


@pytest.mark.asyncio
async def test_checkpoint_is_reloaded_for_next_assignment():
    instance, partitions = checkpoint_consumer()
    await instance._checkpoint_assigned(partitions)
    assert instance._assigned_partitions == partitions
    assert instance._checkpoint_error is None
    assert {args[0].partition: args[1] for args, _ in instance._kafka_consumer.seek.call_args_list} == {0: 12, 1: 18}
    await instance._checkpoint_revoked(partitions)
    assert not instance._assigned_partitions
    assert not instance._marked_offsets
    instance._handler.restore_checkpoint.return_value = {0: 20, 1: 22}
    instance._kafka_consumer.seek.reset_mock()
    await instance._checkpoint_assigned(partitions)
    assert {args[0].partition: args[1] for args, _ in instance._kafka_consumer.seek.call_args_list} == {0: 20, 1: 22}


@pytest.mark.asyncio
@pytest.mark.parametrize('offsets', [{0: 12}, {0: 12, 1: 9}, {0: 12, 1: 41}, {0: 12, 1: -1}, {0: True, 1: 18}, None])
async def test_failed_checkpoint_cannot_partially_admit_or_commit(offsets):
    instance, partitions = checkpoint_consumer()
    instance._handler.restore_checkpoint.return_value = offsets
    await instance._checkpoint_assigned(partitions)
    assert isinstance(instance._checkpoint_error, kafka.CheckpointRestoreError)
    assert not instance._assigned_partitions
    assert not instance._marked_offsets
    assert instance._owner.is_expired
    instance._kafka_consumer.seek.assert_not_called()
    instance._handler.on_error.assert_awaited_once()
    instance._handler.begin_request.assert_not_called()


@pytest.mark.asyncio
async def test_revoke_drains_old_partition_work_before_restore():
    instance, partitions = checkpoint_consumer()
    await instance._checkpoint_assigned(partitions)
    partition = kafka.TopicPartition('events', 0)
    entered = asyncio.Event()
    released = asyncio.Event()

    async def in_flight(_record):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            released.set()

    instance._dispatch_record = in_flight
    await instance._process_record(SimpleNamespace(topic='events', partition=0, offset=12))
    task = instance._partition_tasks[partition]
    await asyncio.wait_for(entered.wait(), 1)
    epoch = instance._assignment_epochs[partition]
    await instance._checkpoint_revoked({partition})
    assert released.is_set() and task.done()
    assert partition not in instance._assigned_partitions
    assert partition not in instance._marked_offsets
    assert instance._assignment_epochs[partition] != epoch
    assert kafka.TopicPartition('events', 1) in instance._assigned_partitions
    assert not task.cancelled()
    assert not instance._message_errors
    from pyservicelib_gorundebug.runtime.context import Context
    await asyncio.wait_for(instance.stop(Context()), 1)
    assert instance._kafka_consumer is None


@pytest.mark.asyncio
async def test_failed_bounds_read_blocks_input_even_if_driver_continues():
    instance, partitions = checkpoint_consumer()
    instance._kafka_consumer.beginning_offsets.side_effect = RuntimeError('broker unavailable')
    await instance._checkpoint_assigned(partitions)
    assert isinstance(instance._checkpoint_error, RuntimeError)
    assert not instance._assigned_partitions
    instance._kafka_consumer.seek.assert_not_called()
    await instance._process_record(SimpleNamespace(topic='events', partition=0, offset=12))
    assert not instance._message_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize('stop_before_ready', [False, True])
async def test_source_start_does_not_wait_for_s3_and_stop_cancels_checkpoint(monkeypatch, stop_before_ready):
    from pyservicelib_gorundebug.runtime.context import Context
    instance = consumer()
    instance._kafka_consumer = None
    entered, installed = asyncio.Event(), asyncio.Event()
    config = SimpleNamespace(enabled=True, id_data_connector=1, topic='events', consumer_group='group')
    instance._endpoint = SimpleNamespace(
        config=config, name='events', environment=SimpleNamespace(
            config=SimpleNamespace(get_data_connector_by_id=lambda _: SimpleNamespace(brokers='fake:9092')),
        ),
    )

    async def restore(_):
        entered.set()
        await installed.wait()
        return {0: 12}

    class Transport:
        start = AsyncMock()
        stop = AsyncMock()
        commit = AsyncMock()
        subscribe = Mock()
        def __aiter__(self): return self
        async def __anext__(self):
            await asyncio.Event().wait()
        def assignment(self): return set()

    transport = Transport()
    factory = Mock(return_value=transport)
    monkeypatch.setattr(kafka, 'AIOKafkaConsumer', factory)
    instance._handler.restore_checkpoint = AsyncMock(side_effect=restore)
    instance._handler.on_error = AsyncMock()
    await asyncio.wait_for(instance.start(Context()), 1)
    await asyncio.wait_for(entered.wait(), 1)
    factory.assert_not_called()
    if not stop_before_ready:
        installed.set()
        await asyncio.wait_for(instance._start_task, 1)
        transport.start.assert_awaited_once()
        assert factory.call_args.kwargs['auto_offset_reset'] == 'none'
    await asyncio.wait_for(instance.stop(Context()), 1)
    if stop_before_ready:
        factory.assert_not_called()
        assert instance._start_task.cancelled()
    instance._handler.on_error.assert_not_called()

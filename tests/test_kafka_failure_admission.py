"""Partition failure and retained-handle regressions, using the real request/lane code."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_kafka_source_lifecycle import consumer
from pyservicelib_gorundebug.datasource.kafka import aiokafkads as kafka
from pyservicelib_gorundebug.runtime.context.request import request_cancelled
from pyservicelib_gorundebug.runtime.store.rotatingmap import RotatingMap


def record(partition=0, offset=7):
    return SimpleNamespace(topic='events', partition=partition, offset=offset,
                           key=b'key', value=b'value')


def request_consumer(has_result=False):
    c = consumer()
    c._assigned_partitions = {kafka.TopicPartition("events", n) for n in (0, 1)}
    c._tracing = c._tracer = None
    c._input_stream = SimpleNamespace(name='input')
    c._endpoint = SimpleNamespace(
        name='events', on_request_start=Mock(return_value=1),
        on_request_end=Mock(), on_pending_add=Mock(), on_pending_remove=Mock(),
        on_begin_request_failed=Mock(), config=SimpleNamespace(topic='events'))
    c._has_result = has_result
    c._pending = RotatingMap(30) if has_result else None
    c._kafka_consumer.pause = Mock()
    c._kafka_consumer.resume = Mock()
    c._kafka_consumer.assignment = lambda: c._assigned_partitions
    c._handler.begin_request = AsyncMock(return_value='state')
    c._handler.end_request = AsyncMock()
    c._handler.concurrency = lambda sc: 2
    return c


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['begin', 'sync', 'async', 'async_throw', 'done_throw', 'done_cancelled'])
async def test_failure_parks_only_failed_partition_and_discards_successors(failure):
    c = request_consumer(failure in ('async', 'async_throw', 'done_throw', 'done_cancelled'))
    entered, release = asyncio.Event(), asyncio.Event()
    seen, handles, callbacks, routed_errors = [], [], [], []
    c._handler.get_message_id = lambda *args: "result-id"
    c._endpoint.on_late_result = Mock()
    c._endpoint.on_missing_stream_id = Mock()
    error = ValueError('failed record')
    async def begin(sc):
        if failure == 'begin' and not entered.is_set():
            entered.set()
            await release.wait()
            raise error
        return 'state'
    c._handler.begin_request.side_effect = begin
    async def consume(sc, state, msg, result):
        seen.append((msg.partition, msg.offset))
        handles.append(msg)
        if msg.partition == 0:
            entered.set()
            await release.wait()
            if failure == 'sync':
                raise error
            if failure in ('async', 'async_throw', 'done_throw'):
                def callback(sc, state, value):
                    assert state == 'state' and value == {'typed': 'result'}
                    if failure == 'done_throw':
                        result.done()
                    if failure in ('async_throw', 'done_throw'):
                        raise error
                    request_cancelled.get().set()
                    return True
                result.set_result_callback('result-id', callback)
                async def deliver():
                    await asyncio.sleep(0)
                    try:
                        await c._consume_result({'typed': 'result'})
                    except ValueError as err:
                        routed_errors.append(err)
                callbacks.append(asyncio.create_task(deliver()))
                return
            request_cancelled.get().set()
            if failure == 'done_cancelled':
                result.done()
        else:
            msg.mark_message()
            await msg.commit()
            result.done()
    c._handler.consume_message = consume
    await c._process_record(record())
    await entered.wait()
    await c._process_record(record(offset=9))
    await c._process_record(record(partition=1))
    release.set()
    await asyncio.wait_for(asyncio.gather(*tuple(c._message_tasks)), 1)
    await asyncio.gather(*callbacks)
    assert routed_errors == ([error] if failure in ("async_throw", "done_throw") else [])
    await c._process_record(record(offset=10))
    assert (0, 9) not in seen and (0, 10) not in seen
    assert (1, 7) in seen
    p = kafka.TopicPartition('events', 0)
    assert c._failed_partitions == {p}
    assert p in c._paused_partitions
    assert c._kafka_consumer.commit.await_args_list == [(( {kafka.TopicPartition('events', 1): 8},), {})]
    assert c._handler.end_request.await_count == (1 if failure == 'begin' else 2)
    if failure != 'begin':
        failed_end = [call for call in c._handler.end_request.await_args_list if call.args[1] is not None]
        assert len(failed_end) == 1
        handles[0].mark_message()
        with pytest.raises(kafka.CheckpointRestoreError):
            await handles[0].commit()
    assert p not in c._marked_offsets
    assert not c._message_errors
    assert c._active_count == 0
    if c._pending:
        assert c._endpoint.on_pending_add.call_count == c._endpoint.on_pending_remove.call_count


@pytest.mark.asyncio
@pytest.mark.parametrize('restoring', [False, True])
async def test_retained_handle_fenced_during_revoke_and_after_reassignment(restoring):
    c = request_consumer()
    c._restoring = restoring
    p = kafka.TopicPartition('events', 0)
    c._handler.restore_checkpoint.return_value = {0: 7}
    c._kafka_consumer.beginning_offsets = AsyncMock(return_value={p: 0})
    c._kafka_consumer.end_offsets = AsyncMock(return_value={p: 100})
    c._kafka_consumer.seek = Mock()
    handles = []
    async def consume(sc, state, msg, result):
        handles.append(msg)
    c._handler.consume_message = consume
    await c._checkpoint_assigned({p})
    await c._dispatch_record(record())
    old = handles[-1]
    old.mark_message()
    await old.commit()
    await c._checkpoint_revoked({p})
    old.mark_message()
    assert p not in c._marked_offsets
    with pytest.raises(kafka.CheckpointRestoreError):
        await old.commit()
    await c._checkpoint_assigned({p})
    before = dict(c._marked_offsets)
    old.mark_message()
    assert c._marked_offsets == before
    with pytest.raises(kafka.CheckpointRestoreError):
        await old.commit()
    await c._dispatch_record(record(offset=9))
    await handles[-1].commit()
    assert c._kafka_consumer.commit.await_count == 2
    if not restoring:
        c._kafka_consumer.seek.assert_not_called()
        c._kafka_consumer.beginning_offsets.assert_not_called()


@pytest.mark.asyncio
async def test_revocation_during_consume_ends_request_once_and_retires_pending():
    c = request_consumer(True)
    entered = asyncio.Event()
    async def consume(*args):
        entered.set()
        await asyncio.Event().wait()
    c._handler.consume_message = consume
    await c._process_record(record())
    await entered.wait()
    await asyncio.wait_for(c._checkpoint_revoked({kafka.TopicPartition('events', 0)}), 1)
    c._handler.end_request.assert_awaited_once()
    assert c._handler.end_request.await_args.args[1] is not None
    c._endpoint.on_pending_remove.assert_called_once()
    assert not c._message_errors


@pytest.mark.asyncio
async def test_commit_receipt_rejects_revocation_during_native_wait():
    c = request_consumer()
    handles = []
    async def consume(sc, state, msg, result):
        handles.append(msg)
    c._handler.consume_message = consume
    await c._dispatch_record(record())
    entered, release = asyncio.Event(), asyncio.Event()
    async def commit(offsets):
        entered.set()
        await release.wait()
    c._kafka_consumer.commit.side_effect = commit
    pending = asyncio.create_task(handles[0].commit())
    await entered.wait()
    p = kafka.TopicPartition('events', 0)
    revoked = asyncio.create_task(c._checkpoint_revoked({p}))
    await asyncio.sleep(0)
    assert p not in c._assigned_partitions
    release.set()
    with pytest.raises(kafka.CheckpointRestoreError, match='during commit'):
        await pending
    await revoked
    c._kafka_consumer.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_assignment_reopens_only_on_reassignment():
    c = request_consumer()
    c._handler.consume_message = AsyncMock(side_effect=ValueError('failed'))
    await c._process_record(record())
    await asyncio.gather(*tuple(c._message_tasks))
    p = kafka.TopicPartition('events', 0)
    assert p in c._failed_partitions
    await c._checkpoint_revoked({p})
    await c._process_record(record(offset=9))
    assert not c._partition_tasks
    await c._checkpoint_assigned({p})
    c._handler.consume_message.side_effect = None
    await c._process_record(record())
    await asyncio.gather(*tuple(c._message_tasks))
    assert c._handler.consume_message.await_count == 2
    assert p not in c._failed_partitions and p not in c._paused_partitions

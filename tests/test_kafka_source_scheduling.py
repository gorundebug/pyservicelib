import asyncio
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.datasource.kafka.aiokafkads import (
    ConsumerMessage,
    _AIOKafkaTypedEndpointConsumer,
)


def _scheduler(limit: int) -> Any:
    scheduler = object.__new__(_AIOKafkaTypedEndpointConsumer)
    scheduler._handler = SimpleNamespace(concurrency=lambda _sc: limit)
    scheduler._sc = cast(Any, object())
    scheduler._stopped = False
    scheduler._active_count = 0
    scheduler._concurrency_changed = asyncio.Condition()
    scheduler._message_tasks = set()
    scheduler._partition_queues = {}
    scheduler._paused_partitions = set()
    scheduler._partition_prefetch = 64
    scheduler._kafka_consumer = None
    return scheduler


@pytest.mark.asyncio
async def test_messages_in_one_partition_remain_ordered() -> None:
    scheduler = _scheduler(0)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    order: list[int] = []

    async def endpoint_request(record: Any) -> None:
        order.append(record.offset)
        if record.offset == 1:
            first_entered.set()
            await release_first.wait()

    scheduler._endpoint_request = endpoint_request
    await scheduler._process_record(SimpleNamespace(topic="events", partition=0, offset=1))
    await first_entered.wait()
    await scheduler._process_record(SimpleNamespace(topic="events", partition=0, offset=2))
    await asyncio.sleep(0)
    assert order == [1]

    release_first.set()
    await asyncio.gather(*tuple(scheduler._message_tasks))
    assert order == [1, 2]


@pytest.mark.asyncio
async def test_concurrency_limit_applies_across_partitions() -> None:
    scheduler = _scheduler(1)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    active = 0
    maximum = 0

    async def endpoint_request(record: Any) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if record.partition == 0:
            first_entered.set()
            await release_first.wait()
        active -= 1

    scheduler._endpoint_request = endpoint_request
    await scheduler._process_record(SimpleNamespace(topic="events", partition=0, offset=1))
    await first_entered.wait()
    await scheduler._process_record(SimpleNamespace(topic="events", partition=1, offset=1))
    await asyncio.sleep(0)
    assert maximum == 1

    release_first.set()
    await asyncio.gather(*tuple(scheduler._message_tasks))
    assert maximum == 1


@pytest.mark.asyncio
async def test_mark_message_is_committed_by_managed_offset_flush() -> None:
    scheduler = _scheduler(0)
    scheduler._marked_offsets = {}
    scheduler._kafka_consumer = SimpleNamespace(commit=AsyncMock())
    record = SimpleNamespace(
        key=b"order-1",
        value=b"{}",
        topic="events",
        partition=2,
        offset=41,
    )
    message = ConsumerMessage(
        record, scheduler._kafka_consumer, scheduler._mark_message
    )

    message.mark_message("processed")
    await scheduler._flush_marked_offsets()

    scheduler._kafka_consumer.commit.assert_awaited_once()
    offsets = scheduler._kafka_consumer.commit.await_args.args[0]
    assert list(offsets.values()) == [42]
    assert scheduler._marked_offsets == {}


@pytest.mark.asyncio
async def test_begin_failure_does_not_start_request_metrics() -> None:
    scheduler = _scheduler(0)
    events: list[str] = []

    async def begin_request(_sc: object) -> None:
        raise RuntimeError("begin failed")

    scheduler._handler = SimpleNamespace(begin_request=begin_request)
    scheduler._has_result = False
    scheduler._pending = None
    scheduler._tracer = None
    scheduler._kafka_consumer = SimpleNamespace()
    scheduler._input_stream = SimpleNamespace(name="orders")
    scheduler._endpoint = SimpleNamespace(
        name="events",
        on_begin_request_failed=lambda error: events.append("begin_failed"),
        on_request_start=lambda: events.append("start"),
        on_request_end=lambda start, error: events.append("end"),
    )
    record = SimpleNamespace(
        key=b"order-1",
        value=b"{}",
        topic="events",
        partition=0,
        offset=1,
    )

    await scheduler._endpoint_request(record)

    assert events == ["begin_failed"]


@pytest.mark.asyncio
async def test_partition_read_is_paused_until_callback_finishes() -> None:
    from unittest.mock import Mock
    from contextvars import ContextVar
    scheduler = _scheduler(1)
    scheduler._partition_prefetch = 1
    from aiokafka.structs import TopicPartition
    scheduler._kafka_consumer = SimpleNamespace(
        pause=Mock(), resume=Mock(),
        assignment=lambda: {TopicPartition("events", 0)},
    )
    variable = ContextVar('kafka_transport_test', default='outside')
    gate = asyncio.Event()
    entered = asyncio.Event()
    seen = []
    async def request(record):
        seen.append(variable.get())
        entered.set()
        await gate.wait()
        seen.append(variable.get())
    scheduler._endpoint_request = request
    token = variable.set('request')
    try:
        await scheduler._process_record(SimpleNamespace(topic='events', partition=0, offset=1))
    finally:
        variable.reset(token)
    scheduler._kafka_consumer.pause.assert_called_once()
    await entered.wait()
    scheduler._kafka_consumer.resume.assert_not_called()
    assert len(scheduler._message_tasks) == 1
    gate.set()
    await asyncio.gather(*tuple(scheduler._message_tasks))
    assert seen == ['request', 'request']
    scheduler._kafka_consumer.resume.assert_called_once()
    assert not scheduler._partition_queues


@pytest.mark.asyncio
async def test_partition_backlog_does_not_allocate_a_task_per_record() -> None:
    scheduler = _scheduler(2)
    gates = [asyncio.Event(), asyncio.Event()]
    started = [asyncio.Event(), asyncio.Event()]
    seen = [[], []]
    async def request(record):
        started[record.partition].set()
        await gates[record.partition].wait()
        seen[record.partition].append(record.offset)
    scheduler._endpoint_request = request
    for offset in range(100):
        for partition in range(2):
            await scheduler._process_record(SimpleNamespace(topic='events', partition=partition, offset=offset))
    await asyncio.gather(*(event.wait() for event in started))
    assert len(scheduler._message_tasks) == 2
    gates[1].set()
    await asyncio.sleep(.01)
    assert seen[1] == list(range(100))
    assert not seen[0]
    gates[0].set()
    await asyncio.gather(*tuple(scheduler._message_tasks))
    assert seen[0] == list(range(100))


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", [False, True])
async def test_partition_cleanup_on_stop_or_rebalance(revoked: bool) -> None:
    from unittest.mock import Mock
    from aiokafka.structs import TopicPartition

    scheduler = _scheduler(1)
    scheduler._partition_prefetch = 1
    assignment = {TopicPartition("events", 0)}
    scheduler._kafka_consumer = SimpleNamespace(
        pause=Mock(), resume=Mock(), assignment=lambda: assignment,
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def request(record):
        entered.set()
        await release.wait()

    scheduler._endpoint_request = request
    await scheduler._process_record(SimpleNamespace(topic="events", partition=0, offset=0))
    await entered.wait()
    tasks = tuple(scheduler._message_tasks)
    if revoked:
        assignment.clear()
        release.set()
    else:
        scheduler._stopped = True
        for task in tasks:
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert scheduler._active_count == 0
    assert not scheduler._partition_queues
    assert not scheduler._paused_partitions
    scheduler._kafka_consumer.resume.assert_not_called()


@pytest.mark.asyncio
async def test_consume_loop_does_not_read_ahead_of_slow_partitions() -> None:
    from aiokafka.structs import TopicPartition

    class Consumer:
        def __init__(self):
            self.paused = set()
            self.fetched = []
            self.changed = asyncio.Event()

        def assignment(self):
            return {TopicPartition("events", n) for n in range(2)}

        def pause(self, partition):
            self.paused.add(partition)

        def resume(self, partition):
            self.paused.discard(partition)
            self.changed.set()

        def __aiter__(self):
            return self

        async def __anext__(self):
            while True:
                for partition in sorted(self.assignment() - self.paused):
                    record = SimpleNamespace(topic=partition.topic,
                        partition=partition.partition, offset=len(self.fetched))
                    self.fetched.append(record)
                    return record
                self.changed.clear()
                await self.changed.wait()

    scheduler = _scheduler(2)
    scheduler._partition_prefetch = 1
    consumer = Consumer()
    scheduler._kafka_consumer = consumer
    entered = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()

    async def request(record):
        entered[record.partition].set()
        await release.wait()

    scheduler._endpoint_request = request
    runner = asyncio.create_task(scheduler._consume_loop())
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered)), 1)
        await asyncio.sleep(0)
        assert len(consumer.fetched) == 2
        assert len(scheduler._message_tasks) == 2
    finally:
        scheduler._stopped = True
        runner.cancel()
        tasks = tuple(scheduler._message_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(runner, *tasks, return_exceptions=True)
    assert not scheduler._partition_queues
    assert scheduler._active_count == 0

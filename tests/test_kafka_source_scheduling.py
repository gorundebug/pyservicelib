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
    scheduler._partition_locks = {}
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

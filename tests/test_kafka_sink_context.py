import asyncio
from types import SimpleNamespace
from typing import Callable
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.datasink.kafka.aiokafkads import (
    SinkMessage,
    _AIOKafkaEndpointConsumer,
)
from pyservicelib_gorundebug.runtime.context.request import (
    stream_id_from_context,
    with_stream_id,
)


class _Handler:
    def get_stream_id(self, value: object) -> str:
        del value
        return "kafka-message-key"

    def begin_request(self, stream: object) -> None:
        del stream

    async def consume_message(
        self,
        stream: object,
        handler_state: None,
        value: object,
        message: object,
    ) -> None:
        del stream, handler_state, value, message

    async def end_request(
        self,
        stream: object,
        error: Exception | None,
        handler_state: None,
    ) -> None:
        del stream, error, handler_state


class _Partitioner:
    def __init__(self) -> None:
        self.call: tuple[object, int] | None = None

    def partition(self, value: object, num_partitions: int) -> int:
        self.call = (value, num_partitions)
        return 2


class _SendingHandler(_Handler):
    async def consume_message(
        self,
        stream: object,
        handler_state: None,
        value: object,
        message: SinkMessage[object],
    ) -> None:
        del stream, handler_state, value
        message.send(lambda partition, offset, error: (partition, offset, error))


class _FailingBeginHandler(_Handler):
    def begin_request(self, stream: object) -> None:
        del stream
        raise ValueError("begin failed")


def test_kafka_delivery_result_uses_runtime_task_registry() -> None:
    scheduled: list[tuple[object, tuple[object, ...]]] = []

    def create_task(fn: object, *args: object) -> None:
        scheduled.append((fn, args))

    def send(
        key: bytes | None,
        value: bytes | None,
        callback: Callable[[int, int, Exception | None], None],
    ) -> None:
        del key, value
        callback(2, 41, None)

    async def collect(result: str) -> None:
        del result

    message = SinkMessage[str]("events", send, collect, create_task)
    message.send(lambda partition, offset, error: f"{partition}:{offset}:{error}")

    assert scheduled == [(collect, ("2:41:None",))]


@pytest.mark.asyncio
async def test_kafka_message_key_does_not_replace_request_stream_id() -> None:
    endpoint = SimpleNamespace(
        name="Kafka endpoint",
        topic="orders",
        datasink=None,
        environment=SimpleNamespace(
            runtime=SimpleNamespace(create_task=lambda *args, **kwargs: None)
        ),
        on_request_start=lambda: 0.0,
        on_request_end=lambda start, error: None,
    )
    consumer = object.__new__(_AIOKafkaEndpointConsumer)
    consumer._endpoint = endpoint
    consumer._stream = SimpleNamespace(name="Kafka stream")
    consumer._handler = _Handler()
    consumer._partitioner = None
    consumer._tracer = None

    with_stream_id("request-correlation-id")
    await consumer.consume(object())

    assert stream_id_from_context() == "request-correlation-id"


@pytest.mark.asyncio
async def test_kafka_custom_partitioner_receives_current_partition_count() -> None:
    value = object()
    partitioner = _Partitioner()
    data_sink = SimpleNamespace(
        partitions_for=AsyncMock(return_value=4),
        send_message=AsyncMock(),
    )
    scheduled: list[asyncio.Task[None]] = []

    def create_task(fn: Callable[[], object]) -> None:
        scheduled.append(asyncio.create_task(fn()))  # type: ignore[arg-type]

    endpoint = SimpleNamespace(
        name="Kafka endpoint",
        topic="orders",
        datasink=data_sink,
        environment=SimpleNamespace(
            runtime=SimpleNamespace(create_task=create_task)
        ),
        on_request_start=lambda: 0.0,
        on_request_end=lambda start, error: None,
    )
    consumer = object.__new__(_AIOKafkaEndpointConsumer)
    consumer._endpoint = endpoint
    consumer._stream = SimpleNamespace(name="Kafka stream")
    consumer._handler = _SendingHandler()
    consumer._partitioner = partitioner
    consumer._tracer = None

    await consumer.consume(value)
    await asyncio.gather(*scheduled)

    assert partitioner.call == (value, 4)
    data_sink.partitions_for.assert_awaited_once_with("orders")


@pytest.mark.asyncio
async def test_kafka_begin_failure_is_not_counted_as_active_request() -> None:
    events: list[tuple[str, object]] = []

    def on_request_start() -> float:
        events.append(("start", None))
        return 0.0

    endpoint = SimpleNamespace(
        name="Kafka endpoint",
        topic="orders",
        datasink=None,
        environment=SimpleNamespace(
            runtime=SimpleNamespace(create_task=lambda *args, **kwargs: None)
        ),
        on_begin_request_failed=lambda error: events.append(("begin", error)),
        on_request_start=on_request_start,
        on_request_end=lambda start, error: events.append(("end", error)),
    )
    consumer = object.__new__(_AIOKafkaEndpointConsumer)
    consumer._endpoint = endpoint
    consumer._stream = SimpleNamespace(name="Kafka stream")
    consumer._handler = _FailingBeginHandler()
    consumer._partitioner = None
    consumer._tracer = None

    await consumer.consume(object())

    assert len(events) == 1
    assert events[0][0] == "begin"
    assert isinstance(events[0][1], ValueError)

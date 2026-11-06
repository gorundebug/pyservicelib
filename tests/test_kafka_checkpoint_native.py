"""Explicit disposable-broker acceptance of the public Kafka endpoint contract."""

import asyncio
import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from pyservicelib_gorundebug.datasource.kafka.aiokafkads import (
    CheckpointRestoreError, make_aiokafka_endpoint_consumer,
)
from pyservicelib_gorundebug.operators.input import InputStream
from pyservicelib_gorundebug.runtime.config import ConfigSettings, ServiceAppConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp, ServiceAppLoader
from tests.test_kafka_transaction_visibility import Handler, Observation, configuration


class CheckpointHandler(Handler):
    def __init__(self, offsets):
        super().__init__()
        self.offsets = offsets
        self.received = asyncio.Queue()
        self.failed = asyncio.Queue()

    def restore_checkpoint(self, sc):
        return dict(self.offsets)

    async def consume_message(self, sc, state, message, result):
        await sc.collect(f"{message.partition}:{message.offset}")
        await self.received.put(message)

    async def on_error(self, sc, error):
        self.errors.append(error)
        await self.failed.put(error)


@asynccontextmanager
async def fixture(tmp_path, monkeypatch):
    brokers = os.getenv("SA_TEST_KAFKA_BROKERS")
    if not brokers:
        pytest.skip("set SA_TEST_KAFKA_BROKERS to a disposable broker")
    topic = "sa-python-checkpoint-" + uuid.uuid4().hex
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    group_reader = AIOKafkaConsumer(bootstrap_servers=brokers, group_id=topic,
                                   enable_auto_commit=False)
    services = []
    partitions = [TopicPartition(topic, p) for p in range(2)]
    try:
        async with asyncio.timeout(90):
            await admin.start()
            await admin.create_topics([NewTopic(topic, 2, 1)])
            await producer.start()
            for partition in range(2):
                for offset in range(5):
                    await producer.send_and_wait(topic, str(offset).encode(), partition=partition)
            await group_reader.start()
            group_reader.assign(partitions)
            await group_reader.commit({p: 2 for p in partitions})

            async def start(offsets):
                config = configuration(brokers, topic)
                config_path = tmp_path / f"config-{len(services)}.json"
                config_path.write_text(json.dumps(config))
                monkeypatch.setattr(sys, "argv", [sys.argv[0], "--config", str(config_path),
                                                  "--values", str(tmp_path / "absent.yaml")])
                service = await ServiceAppLoader[ServiceApp, ServiceAppConfig]().load(
                    "visibility", None, ConfigSettings())
                services.append(service)
                stream = InputStream[str, str, Exception](
                    service.config.get_input_stream_config("input"), service)
                observation = Observation(service)
                stream.consumer = observation
                handler = CheckpointHandler(offsets)
                make_aiokafka_endpoint_consumer(stream, handler)
                await service.get_datasource(1).start(Context(timeout=timedelta(seconds=90)))
                return handler, observation

            yield start, group_reader, partitions
    finally:
        async with asyncio.timeout(20):
            for service in reversed(services):
                await service.stop(Context(timeout=timedelta(seconds=10)))
            await group_reader.stop()
            await producer.stop()
            await admin.delete_topics([topic])
            await admin.close()


@pytest.mark.asyncio
async def test_native_checkpoint_overrides_group_rebalances_and_rejects_old_ack(tmp_path, monkeypatch):
    async with fixture(tmp_path, monkeypatch) as (start, group, partitions):
        first, observed = await start({0: 1, 1: 3})
        messages = [await asyncio.wait_for(first.received.get(), 15) for _ in range(6)]
        assert {(m.partition, m.offset) for m in messages} == {
            (0, 1), (0, 2), (0, 3), (0, 4), (1, 3), (1, 4)}
        assert {await observed.values.get() for _ in messages} == {f"{m.partition}:{m.offset}" for m in messages}
        assert not first.errors
        # Persist group offsets ahead of the durable application checkpoint.
        for message in messages:
            if message.offset == 4:
                message.mark_message()
                await message.commit()
        assert [await group.committed(p) for p in partitions] == [5, 5]
        first.offsets = {0: 4, 1: 4}
        second, observed_second = await start({0: 4, 1: 4})
        restored_first, restored_second = await asyncio.gather(
            asyncio.wait_for(first.received.get(), 20),
            asyncio.wait_for(second.received.get(), 20))
        assert {restored_first.partition, restored_second.partition} == {0, 1}
        assert restored_first.offset == restored_second.offset == 4
        assert await observed.values.get() == f"{restored_first.partition}:4"
        assert await observed_second.values.get() == f"{restored_second.partition}:4"
        for current in (restored_first, restored_second):
            current.mark_message()
            await current.commit()
        old = next(m for m in messages if m.partition == restored_second.partition and m.offset < 4)
        old.mark_message()
        with pytest.raises(CheckpointRestoreError, match="revoked"):
            await old.commit()
        assert [await group.committed(p) for p in partitions] == [5, 5]
        assert not first.errors and not second.errors


@pytest.mark.asyncio
@pytest.mark.parametrize("offsets", [{}, {0: 6, 1: 6}])
async def test_native_invalid_checkpoint_neither_delivers_nor_commits(tmp_path, monkeypatch, offsets):
    async with fixture(tmp_path, monkeypatch) as (start, group, partitions):
        handler, observed = await start(offsets)
        error = await asyncio.wait_for(handler.failed.get(), 15)
        assert isinstance(error, CheckpointRestoreError)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(handler.received.get(), 0.3)
        assert observed.values.empty()
        assert [await group.committed(p) for p in partitions] == [2, 2]

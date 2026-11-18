"""Opt-in B01 acceptance: real group rebalances and broker committed offsets."""
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
from pyservicelib_gorundebug.operators.map import MapStream
from pyservicelib_gorundebug.runtime.config import ConfigSettings, ServiceAppConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import request_cancelled
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp, ServiceAppLoader
from tests.test_kafka_transaction_visibility import Handler, configuration


async def eventually(predicate, timeout=25):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.02)


class Identity:
    async def map(self, stream, value, out):
        await out.out(value)


class FailureHandler(Handler):
    def __init__(self, restore, failure=None):
        super().__init__()
        self.restore = restore
        self.failure = failure
        self.seen = []
        self.handles = []
        self.ends = []
        self.tasks = []
        self.buffered_at_admission = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def restore_checkpoint(self, sc):
        return {0: 0, 1: 0} if self.restore else None

    async def begin_request(self, sc):
        return {}

    async def consume_message(self, sc, state, message, result):
        key = (message.partition, message.offset)
        state['key'] = key
        self.seen.append(key)
        self.handles.append(message)
        if message.partition == 0 and self.failure:
            self.entered.set()
            if self.failure == 'sync':
                await self.release.wait()
                raise ValueError('test-owned p0 failure')
            def failed(sc, state, value):
                request_cancelled.get().set()
                return True
            result.set_result_callback('failure', failed)
            async def deliver():
                await self.release.wait()
                await sc.collect('failure')
            self.tasks.append(asyncio.create_task(deliver()))
            return
        message.mark_message()
        await message.commit()
        result.done()

    async def end_request(self, sc, error, state):
        self.ends.append((state['key'], error))


@asynccontextmanager
async def fixture(tmp_path, monkeypatch):
    brokers = os.getenv('SA_TEST_KAFKA_BROKERS')
    if not brokers:
        pytest.skip('set SA_TEST_KAFKA_BROKERS to a disposable broker')
    topic = 'sa-python-failure-' + uuid.uuid4().hex
    partitions = [TopicPartition(topic, p) for p in range(2)]
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    reader = AIOKafkaConsumer(bootstrap_servers=brokers, group_id=topic,
                             enable_auto_commit=False)
    services, handlers = [], []
    created = False
    try:
        async with asyncio.timeout(100):
            await admin.start()
            await admin.create_topics([NewTopic(topic, 2, 1)])
            created = True
            await producer.start()
            await reader.start()
            reader.assign(partitions)
            await reader.commit({p: 0 for p in partitions})

            async def send(partition, count):
                for _ in range(count):
                    await producer.send_and_wait(topic, b'value', partition=partition)

            async def start(restore, failure=None):
                config_path = tmp_path / f'{len(services)}.json'
                config_path.write_text(json.dumps(configuration(brokers, topic)))
                monkeypatch.setattr(sys, 'argv', [sys.argv[0], '--config', str(config_path),
                                               '--values', str(tmp_path / 'absent.yaml')])
                service = await ServiceAppLoader[ServiceApp, ServiceAppConfig]().load(
                    'visibility', None, ConfigSettings())
                services.append(service)
                stream = InputStream[str, str, Exception](
                    service.config.get_input_stream_config('input'), service)
                if failure == 'async':
                    result = MapStream(service.config.get_map_stream_config('observed'), stream, Identity())
                    stream.set_source(result)
                handler = FailureHandler(restore, failure)
                handlers.append(handler)
                endpoint = make_aiokafka_endpoint_consumer(stream, handler)
                original_admit = endpoint._process_record
                async def observe_admission(record):
                    # Read-only test instrumentation before the SDK pauses the
                    # partition. aiokafka discards cached batches when the next
                    # iterator call observes that pause; handler entry is later.
                    if record.partition == 0:
                        native = endpoint._kafka_consumer
                        buffered = native._fetcher._records.get(partitions[0])
                        handler.buffered_at_admission.append((
                            buffered is not None and buffered.has_more(),
                            native.highwater(partitions[0]), record.offset))
                    await original_admit(record)
                endpoint._process_record = observe_admission
                await service.get_datasource(1).start(Context(timeout=timedelta(seconds=100)))
                return service, endpoint, handler

            yield start, send, reader, partitions
    finally:
        async with asyncio.timeout(25):
            for handler in handlers:
                handler.release.set()
                await asyncio.gather(*handler.tasks, return_exceptions=True)
            for service in reversed(services):
                await service.stop(Context(timeout=timedelta(seconds=8)))
            await reader.stop()
            await producer.stop()
            if created:
                await admin.delete_topics([topic])
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('restore', [False, True], ids=['ordinary', 'restore'])
@pytest.mark.parametrize('failure', ['sync', 'async'])
async def test_native_failed_partition_cannot_be_overtaken(tmp_path, monkeypatch, restore, failure):
    async with fixture(tmp_path, monkeypatch) as (start, send, reader, partitions):
        # A batch is available before subscription: exercise native fetch buffering.
        await send(0, 32)
        await send(1, 4)
        _, endpoint, handler = await start(restore, failure)
        await asyncio.wait_for(handler.entered.wait(), 20)
        assert handler.buffered_at_admission == [(True, 32, 0)]
        assert await endpoint._kafka_consumer.position(partitions[0]) == 1
        await eventually(lambda: (1, 3) in [key for key, error in handler.ends])
        assert await reader.committed(partitions[1]) == 4
        handler.release.set()
        await eventually(lambda: any(key == (0, 0) and error is not None for key, error in handler.ends))
        # New traffic after failure, plus >2 periodic offset flush intervals.
        await send(0, 8)
        await send(1, 4)
        await eventually(lambda: (1, 7) in [key for key, error in handler.ends])
        await asyncio.sleep(2.2)
        assert [key for key in handler.seen if key[0] == 0] == [(0, 0)]
        assert [await reader.committed(p) for p in partitions] == [0, 8]
        assert partitions[0] in endpoint._kafka_consumer.paused()
        assert not endpoint._partition_queues
        assert len(handler.ends) == 9
        assert len([error for _, error in handler.ends if error is not None]) == 1
        assert not handler.errors
        old = next(m for m in handler.handles if m.partition == 0)
        old.mark_message()
        with pytest.raises(CheckpointRestoreError):
            await old.commit()
        await asyncio.sleep(1.1)
        assert await reader.committed(partitions[0]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('restore', [False, True], ids=['ordinary', 'restore'])
async def test_native_retained_ack_fenced_after_real_revoke_reassign(tmp_path, monkeypatch, restore):
    async with fixture(tmp_path, monkeypatch) as (start, send, reader, partitions):
        await send(0, 2)
        await send(1, 2)
        _, first, handler = await start(restore)
        await eventually(lambda: len(handler.ends) == 4)
        old = list(handler.handles)
        epochs = dict(first._assignment_epochs)
        second_service, second, other = await start(restore)
        await eventually(lambda: len(first._assigned_partitions) == len(second._assigned_partitions) == 1
                         and all(first._assignment_epochs.get(p, 0) > epochs[p] for p in partitions))
        # Validate a revoked handle AND a handle whose partition returned to the
        # original member: membership alone is insufficient, epoch must fence it.
        for message in old:
            before = dict(first._marked_offsets)
            message.mark_message()
            assert first._marked_offsets == before
            with pytest.raises(CheckpointRestoreError):
                await message.commit()
        await asyncio.sleep(1.2)
        assert [await reader.committed(p) for p in partitions] == [2, 2]
        await second_service.stop(Context(timeout=timedelta(seconds=10)))
        await eventually(lambda: first._assigned_partitions == set(partitions))
        for message in old:
            before = dict(first._marked_offsets)
            message.mark_message()
            assert first._marked_offsets == before
            with pytest.raises(CheckpointRestoreError):
                await message.commit()
        await send(0, 1)
        await send(1, 1)
        await eventually(lambda: {(0, 2), (1, 2)} <= {key for key, error in handler.ends})
        await asyncio.sleep(1.2)
        assert [await reader.committed(p) for p in partitions] == [3, 3]
        assert not handler.errors and not other.errors

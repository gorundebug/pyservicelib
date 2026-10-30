"""Opt-in broker acceptance through the public factory and a real InputStream.

SA_TEST_KAFKA_BROKERS must name a disposable Kafka supporting transactions.
The producer is a test driver; consumption uses the unmodified SDK transport.
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import timedelta

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from pyservicelib_gorundebug.datasource.kafka.aiokafkads import make_aiokafka_endpoint_consumer
from pyservicelib_gorundebug.operators.input import InputStream
from pyservicelib_gorundebug.runtime.common import ServiceStream, StreamConsumer
from pyservicelib_gorundebug.runtime.config import ConfigSettings, ServiceAppConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp, ServiceAppLoader


class Observation(ServiceStream, StreamConsumer[str]):
    def __init__(self, service):
        super().__init__(stream_id=2, env=service)
        self.values = asyncio.Queue()

    @property
    def stream(self):
        return self

    @property
    def type_name(self):
        return "string"

    async def consume(self, value):
        await self.values.put(value)


class Handler:
    def __init__(self):
        self.errors = []

    def concurrency(self, sc):
        return 2

    async def begin_request(self, sc):
        return None

    async def consume_message(self, sc, state, message, result):
        await sc.collect(message.value.decode())
        message.mark_message()

    def get_message_id(self, sc, state, value):
        return value

    async def on_error(self, sc, error):
        self.errors.append(error)

    async def end_request(self, sc, error, state):
        if error is not None:
            self.errors.append(error)


def configuration(brokers, topic):
    return {
        "settings": {"name": "kafka-visibility"},
        "services": [{
            "id": 1, "name": "visibility", "programmingLanguage": 3,
            "modulePath": "visibility", "defaultCallSemantics": 2,
            "httpHost": "127.0.0.1", "httpPort": 0, "grpcHost": "127.0.0.1", "grpcPort": 9201,
            "statusHandler": "", "metricsHandler": "", "startupHandler": "",
            "readinessHandler": "", "livenessHandler": "", "kubernetesWorkloadType": "Deployment",
            "defaultGrpcTimeout": 0, "shutdownTimeout": 10000, "color": "#000000", "environment": "local",
        }],
        "streams": [
            {"id": 1, "name": "input", "idSource": 0, "type": 1, "idService": 1,
             "xPos": 0, "yPos": 0, "valueType": "string", "idEndpoint": 1},
            {"id": 2, "name": "observed", "idSource": 1, "type": 2, "idService": 1,
             "xPos": 1, "yPos": 0, "valueType": "string"},
        ],
        "links": [{"from": 1, "to": 2, "callSemantics": 2}],
        "dataConnectors": [{"id": 1, "name": "kafka", "type": 3,
                            "implementation": "aiokafka", "brokers": brokers}],
        "endpoints": [{"id": 1, "name": "events", "idDataConnector": 1, "functionName": "receive",
                       "enabled": True, "createTopic": False, "topic": topic, "partitions": 2,
                       "consumerGroup": topic, "replicationFactor": 1}],
        "types": [], "pools": [], "modules": [],
    }


@pytest.mark.asyncio
async def test_native_endpoint_transaction_visibility(tmp_path, monkeypatch):
    brokers = os.environ.get("SA_TEST_KAFKA_BROKERS")
    if not brokers:
        pytest.skip("set SA_TEST_KAFKA_BROKERS for native Kafka acceptance")
    topic = "sa-python-visibility-" + uuid.uuid4().hex
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configuration(brokers, topic)))
    monkeypatch.setattr(sys, "argv", [sys.argv[0], "--config", str(config),
                                     "--values", str(tmp_path / "absent-values.yaml")])
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    ordinary = AIOKafkaProducer(bootstrap_servers=brokers)
    transactional = AIOKafkaProducer(bootstrap_servers=brokers, transactional_id=topic)
    service = None
    ctx = Context(timeout=timedelta(seconds=60))
    try:
        async with asyncio.timeout(60):
            await admin.start()
            await admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
            service = await ServiceAppLoader[ServiceApp, ServiceAppConfig]().load(
                "visibility", None, ConfigSettings())
            stream = InputStream[str, str, Exception](service.config.get_input_stream_config("input"), service)
            observation = Observation(service)
            stream.consumer = observation
            handler = Handler()
            make_aiokafka_endpoint_consumer(stream, handler)
            await service.get_datasource(1).start(ctx)
            await ordinary.start()
            await transactional.start()

            async def send(producer, value, partition=0):
                await producer.send_and_wait(topic, value.encode(), partition=partition)

            async def expect(value):
                assert await asyncio.wait_for(observation.values.get(), 10) == value
                assert not handler.errors

            await send(ordinary, "baseline")
            await expect("baseline")
            await transactional.begin_transaction()
            await send(transactional, "aborted")
            await transactional.abort_transaction()
            await send(ordinary, "after-abort")
            await expect("after-abort")

            await transactional.begin_transaction()
            await send(transactional, "open-then-committed")
            await send(ordinary, "behind-open")
            # Another partition proves the SDK is actively consuming while
            # partition 0 remains blocked at its last stable offset.
            await send(ordinary, "other-partition", partition=1)
            await expect("other-partition")
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(observation.values.get(), 0.5)
            await transactional.commit_transaction()
            await expect("open-then-committed")
            await expect("behind-open")

            # Reuse the exact same transactional producer after another abort.
            await transactional.begin_transaction()
            await send(transactional, "aborted-again")
            await transactional.abort_transaction()
            async with transactional.transaction():
                await send(transactional, "reused-producer-commit")
            await send(ordinary, "final-barrier")
            await expect("reused-producer-commit")
            await expect("final-barrier")
            assert observation.values.empty()
            assert not handler.errors
    finally:
        # Keep cleanup bounded independently of an expired source context.
        async with asyncio.timeout(15):
            await transactional.stop()
            await ordinary.stop()
            if service is not None:
                await service.stop(Context(timeout=timedelta(seconds=10)))
            await admin.delete_topics([topic])
            await admin.close()

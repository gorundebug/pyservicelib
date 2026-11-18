import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from pyservicelib_gorundebug.runtime import config
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as RecordingMetrics


def fixture(protocol):
    module = importlib.import_module(f"pyservicelib_gorundebug.datasink.{protocol}.{protocol}ds")
    prefix, address, options = {
        "pulsar": ("Pulsar", "pulsar://localhost", {"adminUrl": "http://localhost"}),
        "redisstreams": ("RedisStreams", "redis://localhost", {}),
        "rabbitmq": ("RabbitMQ", "amqp://localhost", {}),
    }[protocol]
    dc = getattr(config, prefix + "DataConnectorConfig")(id=1, name="events", servers=[address], **options)
    ep = getattr(config, prefix + "EndpointConfig")(id=2, name="publish", idDataConnector=1, functionName="Publish", **{"pulsar": {"topic": "persistent://public/default/events"}, "redisstreams": {"streamKey": "events"}, "rabbitmq": {"queueName": "events"}}[protocol])
    metrics = RecordingMetrics()
    env = SimpleNamespace(metrics=metrics, config=SimpleNamespace(get_endpoint_config_by_id=lambda _: ep), log=Mock())
    sink = SimpleNamespace(environment=env, name="events", data_connector=dc)
    endpoint = module._Endpoint(sink, 2)
    endpoint.requests = 1
    endpoint.accepting = True
    return module, endpoint, metrics


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["pulsar", "redisstreams", "rabbitmq"])
@pytest.mark.parametrize("outcome", ["accepted", "rejected", "unknown"])
async def test_native_outcome_boundary_and_local_refusal(protocol, outcome):
    module, endpoint, metrics = fixture(protocol)
    future = asyncio.get_running_loop().create_future()
    async def publish(*_):
        return await future
    endpoint.session = SimpleNamespace(limit=10, publish=publish)
    def count(label):
        return metrics.counter("broker_endpoint_publication_outcomes_total", {
            "protocol": protocol, "connector": "events", "endpoint": "publish", "outcome": label,
        }).count()
    results = []
    async def collect(value):
        results.append(value)
    def callback(delivery):
        assert count(outcome) == 1  # precedes user conversion
        return delivery
    message = module.SinkMessage(endpoint, collect)
    if protocol == "redisstreams":
        message.fields = [(b"value", b"event")]
    message.send(callback)
    await asyncio.sleep(0)
    assert count(outcome) == 0
    native = outcome.title() if protocol == "pulsar" else outcome
    delivery = module.Delivery(native)
    future.set_result(delivery)
    await asyncio.gather(*endpoint.deliveries)
    assert results == [delivery]
    assert count(outcome) == 1
    # Admission refusal remains separate from the completed native outcome.
    endpoint.requests = 0
    with pytest.raises(module.AdmissionRejected):
        module.SinkMessage(endpoint, collect).send(callback)
    assert count("not-sent") == 1
    assert count(outcome) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["pulsar", "redisstreams", "rabbitmq"])
async def test_sync_records_once_without_implicit_graph_result(protocol):
    module, endpoint, metrics = fixture(protocol)
    delivery = module.Delivery("Accepted" if protocol == "pulsar" else "accepted")
    async def publish(*_):
        return delivery
    endpoint.session = SimpleNamespace(limit=10, publish=publish)
    results = []
    async def collect(value):
        results.append(value)
    message = module.SinkMessage(endpoint, collect)
    if protocol == "redisstreams":
        message.fields = [(b"value", b"event")]
    assert await message.send_sync() is delivery
    assert results == []
    assert metrics.counter("broker_endpoint_publication_outcomes_total", {
        "protocol": protocol, "connector": "events", "endpoint": "publish", "outcome": "accepted",
    }).count() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["accepted", "flushed", "jetstream"])
async def test_nats_native_completion_not_queue_admission(mode):
    from contextvars import copy_context
    from pyservicelib_gorundebug.datasink.nats import natsds as module
    from unittest.mock import AsyncMock

    jetstream = mode == "jetstream"
    dc_class = config.JetStreamDataConnectorConfig if jetstream else config.NatsDataConnectorConfig
    ep_class = config.JetStreamEndpointConfig if jetstream else config.NatsEndpointConfig
    dc = dc_class(id=1, name="events", servers=["nats://localhost"])
    ep = ep_class(id=2, name="publish", idDataConnector=1, functionName="Publish", subject="events", **({"streamName": "events"} if jetstream else {}))
    metrics = RecordingMetrics()
    env = SimpleNamespace(metrics=metrics, config=SimpleNamespace(
        get_data_connector_by_id=lambda _: dc, get_endpoint_config_by_id=lambda _: ep), log=Mock())
    sink = module.NatsDataSink(1, env)
    endpoint = module._Endpoint(sink, 2)
    gate = asyncio.get_running_loop().create_future()
    entered = asyncio.Event()
    async def publish(*args, **kwargs):
        entered.set()
        await gate
    async def publish_async(*args, **kwargs):
        entered.set()
        return gate
    sink.connection = SimpleNamespace(
        timeout=2, nc=SimpleNamespace(is_connected=True, pending_data_size=0, publish=publish),
        js=SimpleNamespace(publish_async=publish_async) if jetstream else None,
        flush=AsyncMock(),
    )
    protocol = "jetstream" if jetstream else "nats"
    label = {"accepted": "client-accepted", "flushed": "flushed", "jetstream": "acknowledged"}[mode]
    def count():
        return metrics.counter("broker_endpoint_publication_outcomes_total", {
            "protocol": protocol, "connector": "events", "endpoint": "publish", "outcome": label,
        }).count()
    def callback(delivery):
        assert count() == 1
        return delivery
    collect = AsyncMock()
    future = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(sink._deliver((endpoint, b"event", {}, callback, collect, mode == "flushed", future, copy_context())))
    await entered.wait()
    assert count() == 0
    gate.set_result(SimpleNamespace(stream="events", seq=1))
    await task
    assert count() == 1
    assert future.result().publication.kind == mode
    collect.assert_awaited_once()


@pytest.mark.asyncio
async def test_publication_outcomes_use_real_prometheus_family():
    from prometheus_client.parser import text_string_to_metric_families
    from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
    from pyservicelib_gorundebug.runtime.brokermetrics import PublicationMetrics
    from pyservicelib_gorundebug.runtime.telemetry import create_prometheus_metrics_engine

    profiles = {
        "NATS": ("nats", ["client-accepted", "flushed", "error"]),
        "JetStream": ("jetstream", ["acknowledged", "error"]),
        "Pulsar": ("pulsar", ["accepted", "not-sent", "rejected", "unknown"]),
        "RedisStreams": ("redisstreams", ["accepted", "not-sent", "rejected", "unknown"]),
        "RabbitMQ": ("rabbitmq", ["accepted", "not-sent", "rejected", "unknown", "unroutable", "modified"]),
    }
    engine = create_prometheus_metrics_engine("broker-export")
    try:
        expected = set()
        for kind, (protocol, outcomes) in profiles.items():
            endpoint = SimpleNamespace(name="publish", environment=SimpleNamespace(metrics=engine.metrics),
                datasink=SimpleNamespace(name="events", data_connector=SimpleNamespace(type=getattr(DataConnectorType, kind))))
            publication = PublicationMetrics(endpoint)
            for outcome in outcomes:
                publication.observe(outcome)
                expected.add((protocol, outcome))
        text = engine.metrics_handler().decode()
        samples = [s for f in text_string_to_metric_families(text) for s in f.samples
                   if s.name == "broker_endpoint_publication_outcomes_total" and s.value > 0]
        assert {(s.labels["protocol"], s.labels["outcome"]) for s in samples} == expected
        assert len(samples) == 19
        assert all(s.value == 1 and s.labels["connector"] == "events" and s.labels["endpoint"] == "publish" for s in samples)
        assert "publication_outcomes_total_total" not in text
    finally:
        await engine.shutdown()

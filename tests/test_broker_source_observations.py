import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.runtime.brokermetrics import SourceMetrics
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as RecordingMetrics

@pytest.mark.asyncio
@pytest.mark.parametrize('protocol,kind,prefix', [('nats','NATS','Nats'), ('jetstream','JetStream','Nats'), ('pulsar','Pulsar','Pulsar'), ('redisstreams','RedisStreams','RedisStreams'), ('rabbitmq','RabbitMQ','RabbitMQ')])
async def test_observations_precede_handler_and_use_native_metadata(protocol, kind, prefix):
    name = 'nats' if protocol == 'jetstream' else protocol
    module = importlib.import_module(f'pyservicelib_gorundebug.datasource.{name}.{name}ds')
    metrics = RecordingMetrics()
    endpoint = SimpleNamespace(name='read', environment=SimpleNamespace(metrics=metrics), datasource=SimpleNamespace(name='events',data_connector=SimpleNamespace(type=getattr(DataConnectorType,kind))))
    observations = SourceMetrics(endpoint)
    consumer = object.__new__(getattr(module,prefix+'EndpointConsumer'))
    consumer.observations = observations
    seen = []
    def count(label):
        return metrics.counter('broker_endpoint_source_observations_total', {'protocol':protocol,'connector':'events','endpoint':'read','observation':label}).count()
    async def inner(*args): seen.append(count('received'))
    consumer._request_inner = AsyncMock(side_effect=inner)
    for index in range(2):
        native = SimpleNamespace(headers={}, metadata=SimpleNamespace(num_delivered=index+1), message=SimpleNamespace(application_properties={}, delivery_count=index, redelivery_count=lambda: index))
        await consumer._request(native)
    assert seen == [1,2]
    if protocol in ('jetstream','pulsar','rabbitmq'): assert count('redelivered') == 1
    assert 'acknowledged' not in observations.counters
    if protocol == 'redisstreams':
        for _ in range(3): observations.observe('reclaimed')
        assert count('reclaimed') == 3
    assert count('received') == 2


def test_disabled_source_metrics_keep_no_series():
    endpoint = SimpleNamespace(name='read', environment=SimpleNamespace(metrics=SimpleNamespace(enabled=False)), datasource=SimpleNamespace(name='events',data_connector=SimpleNamespace(type=DataConnectorType.JetStream)))
    observations = SourceMetrics(endpoint)
    assert observations.counters == {}
    observations.observe('received')


@pytest.mark.asyncio
async def test_source_observations_use_real_prometheus_family():
    from prometheus_client.parser import text_string_to_metric_families
    from pyservicelib_gorundebug.runtime.telemetry import create_prometheus_metrics_engine
    profiles = {
        'NATS': ('nats', ['received']),
        'JetStream': ('jetstream', ['received', 'redelivered']),
        'Pulsar': ('pulsar', ['received', 'redelivered']),
        'RedisStreams': ('redisstreams', ['received', 'reclaimed']),
        'RabbitMQ': ('rabbitmq', ['received', 'redelivered']),
    }
    engine = create_prometheus_metrics_engine('broker-source-export')
    try:
        expected = set()
        for kind, (protocol, observations) in profiles.items():
            endpoint = SimpleNamespace(name='read', environment=SimpleNamespace(metrics=engine.metrics),
                datasource=SimpleNamespace(name='events', data_connector=SimpleNamespace(type=getattr(DataConnectorType, kind))))
            source = SourceMetrics(endpoint)
            for observation in observations:
                source.observe(observation)
                expected.add((protocol, observation))
        text = engine.metrics_handler().decode()
        samples = [s for f in text_string_to_metric_families(text) for s in f.samples
                   if s.name == 'broker_endpoint_source_observations_total' and s.value > 0]
        assert {(s.labels['protocol'], s.labels['observation']) for s in samples} == expected
        assert len(samples) == 9
        assert all(s.value == 1 and s.labels['connector'] == 'events' and s.labels['endpoint'] == 'read' for s in samples)
        assert 'source_observations_total_total' not in text
    finally:
        await engine.shutdown()

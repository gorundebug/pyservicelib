from types import SimpleNamespace

import pytest
from prometheus_client.parser import text_string_to_metric_families

from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.runtime.datasink import DataSinkEndpoint
from pyservicelib_gorundebug.runtime.datasource import DataSourceEndpoint
from pyservicelib_gorundebug.runtime.telemetry import create_prometheus_metrics_engine
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as MetricsFixture


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_real_endpoint_factories_keep_protocol_label_for_every_connector(reverse):
    engine = create_prometheus_metrics_engine("Endpoint schema")
    captured = []

    class RecordingMetrics(MetricsFixture):
        def scope(self, prefix, labels):
            captured.append((prefix, dict(labels)))
            return engine.metrics.scope(prefix, labels)

    metrics = RecordingMetrics()
    kinds = [DataConnectorType.HTTP, DataConnectorType.gRPC, DataConnectorType.Kafka,
             DataConnectorType.Custom, DataConnectorType.Cron, DataConnectorType.Temporal]
    if reverse:
        kinds.reverse()
    try:
        for kind in kinds:
            name = kind.name
            config = SimpleNamespace(get_endpoint_config_by_id=lambda _, name=name: SimpleNamespace(name=name))
            connector = SimpleNamespace(
                name=name, data_connector=SimpleNamespace(type=kind),
                environment=SimpleNamespace(config=config, metrics=metrics),
            )
            for factory in (DataSourceEndpoint, DataSinkEndpoint):
                endpoint = factory(connector, kind.value)
                endpoint.on_request_end(endpoint.on_request_start(), None)

        assert len(captured) == 12
        for _, labels in captured:
            assert set(labels) == {"connector", "endpoint", "protocol"}
            assert labels["protocol"] == ("grpc" if labels["endpoint"] == "gRPC" else "")
        samples = [sample for family in text_string_to_metric_families(engine.metrics_handler().decode())
                   for sample in family.samples
                   if sample.name in {"datasource_endpoint_messages_total", "datasink_endpoint_messages_total"}]
        assert len(samples) == 12
        for sample in samples:
            assert sample.value == 1
            assert sample.labels["protocol"] == ("grpc" if sample.labels["endpoint"] == "gRPC" else "")
    finally:
        await engine.shutdown()

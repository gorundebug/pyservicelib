import pytest

from pyservicelib_gorundebug.runtime.config import (
    RedisStreamsDataConnectorConfig as Connector,
)
from pyservicelib_gorundebug.runtime.config import (
    RedisStreamsEndpointConfig as Endpoint,
)
from pyservicelib_gorundebug.runtime.config import (
    data_connector_config_to_api,
    data_connector_protocol,
    endpoint_config_to_api,
)


def connector(**kw):
    return Connector(
        **({"id": 1, "name": "redis", "servers": ["redis://localhost"]} | kw)
    )


def endpoint(**kw):
    return Endpoint(
        **(
            {
                "id": 2,
                "name": "stream",
                "idDataConnector": 1,
                "functionName": "Handle",
                "streamKey": "literal*key",
            }
            | kw
        )
    )


@pytest.mark.parametrize(
    "kw",
    [
        {"servers": []},
        {"servers": ["redis://a", "redis://b"]},
        {"servers": ["sentinel://a"]},
        {"servers": ["redis://a:0"]},
        {"servers": ["redis://a:65536"]},
        {"servers": ["redis://a:"]},
        {"servers": ["redis://a/"]},
        {"servers": ["redis://a/1"]},
        {"servers": ["redis://a?"]},
        {"servers": ["redis://a#"]},
        {"servers": ["redis://u:p@a"]},
        {"servers": ["redis://@a"]},
        {"servers": ["redis://a "]},
        {"redisDatabase": -1},
        {"redisDatabase": True},
        {"connectTimeoutMs": 0},
        {"operationTimeoutMs": 2147483648},
        {"username": "u"},
        {"token": "x"},
        {"credentialsFile": "x"},
        {"tlsCaFile": "x"},
        {"tlsEnabled": True, "tlsCertFile": "x"},
        {"tlsEnabled": True, "tlsServerName": "other"},
        {"implementation": "aioredis"},
    ],
)
def test_connector_rejections(kw):
    with pytest.raises(ValueError):
        connector(**kw)


@pytest.mark.parametrize(
    "kw",
    [
        {"readCount": 0},
        {"readCount": True},
        {"readCount": 4097},
        {"readBlockMs": 60001},
        {"claimIdleMs": 0},
        {"claimIntervalMs": 0},
        {"publisherMaxInFlight": 0},
        {"groupStartId": "1-0"},
    ],
)
def test_endpoint_bounds(kw):
    with pytest.raises(ValueError):
        endpoint(**kw)


def test_roles_defaults_and_projection():
    dc = connector(password="runtime", redisDatabase=1, tlsEnabled=False)
    assert data_connector_protocol(dc.type) == "redisstreams"
    assert dc.connect_timeout_ms == 2000 and dc.operation_timeout_ms == 5000
    api = data_connector_config_to_api(dc)
    assert api.redis_database == 1 and api.tls_enabled is False
    assert api.connect_timeout_ms == 2000
    api.servers.append("redis://other")
    assert len(dc.servers) == 1
    ep = endpoint(
        consumerGroup="workers",
        createConsumerGroup=False,
        enabled=False,
        tracingEnabled=False,
    )
    ep.validate_role(True)
    native = endpoint_config_to_api(ep)
    assert native.stream_key == "literal*key" and native.create_consumer_group is False
    assert native.create_stream is None and native.read_count is None
    assert native.enabled is False and native.tracing_enabled is False
    with pytest.raises(ValueError):
        ep.validate_role(False)
    with pytest.raises(ValueError):
        endpoint(consumerGroup="g", createStream=False).validate_role(True)
    with pytest.raises(ValueError):
        endpoint(createConsumerGroup=False).validate_role(False)
    with pytest.raises(ValueError):
        endpoint(consumerGroup="g", groupStartId="$").validate_role(True)
    for start in ("0-0", "$"):
        endpoint(
            consumerGroup="g", createConsumerGroup=True, groupStartId=start
        ).validate_role(True)
    sink = endpoint(createStream=False, publisherMaxInFlight=1)
    sink.validate_role(False)
    assert endpoint_config_to_api(sink).create_stream is False


@pytest.mark.parametrize(
    "kw",
    [
        {"consumerCredits": 256},
        {"createQueue": False},
        {"createConsumer": False},
        {"queueName": "q"},
        {"subject": "s"},
    ],
)
def test_rejects_other_broker_options_even_false(kw):
    with pytest.raises(ValueError):
        endpoint(**kw).validate_role(False)


def test_validation_does_not_include_credentials():
    with pytest.raises(ValueError) as caught:
        connector(servers=["redis://user:SECRET@localhost"], password="PASSWORD")
    assert "SECRET" not in str(caught.value) and "PASSWORD" not in str(caught.value)


def test_runtime_yaml_preserves_redis_fields_and_false_absence():
    import yaml

    from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
    from pyservicelib_gorundebug.api.models.stream_app import StreamApp
    from pyservicelib_gorundebug.runtime.config import app_to_yaml

    dc = connector(redisDatabase=0, tlsEnabled=False)
    ep = endpoint(
        consumerGroup="g",
        createConsumerGroup=False,
        readCount=1,
        readBlockMs=5,
        claimIdleMs=2,
        claimIntervalMs=3,
        consumerName="instance",
        enabled=False,
        tracingEnabled=False,
    )
    app = StreamApp(
        settings=ProjectSettings(name="Redis"),
        services=[],
        streams=[],
        links=[],
        types=[],
        pools=[],
        dataConnectors=[data_connector_config_to_api(dc)],
        endpoints=[endpoint_config_to_api(ep)],
    )
    doc = yaml.safe_load(app_to_yaml(app))
    native = doc["dataConnectors"]["redis"]
    assert native["redisDatabase"] == 0 and native["tlsEnabled"] is False
    assert native["servers"] == ["redis://localhost"] and "virtualHost" not in native
    read = native["endpoints"]["stream"]
    assert read["createConsumerGroup"] is False and "createStream" not in read
    assert (
        read["readCount"] == 1
        and read["claimIntervalMs"] == 3
        and read["consumerName"] == "instance"
    )
    assert read["enabled"] is False and read["tracingEnabled"] is False

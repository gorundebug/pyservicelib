import pytest
from pydantic import ValidationError

from pyservicelib_gorundebug.runtime.config import PulsarDataConnectorConfig as DC
from pyservicelib_gorundebug.runtime.config import PulsarEndpointConfig as EP
from pyservicelib_gorundebug.runtime.config.config_to_api import (
    data_connector_config_to_api,
    endpoint_config_to_api,
)


def dc(**kw):
    return DC(
        **(
            {
                "id": 1,
                "name": "p",
                "servers": ["pulsar://localhost:16650"],
                "adminUrl": "http://localhost:18080",
            }
            | kw
        )
    )


def ep(**kw):
    return EP(
        **(
            {
                "id": 2,
                "name": "p",
                "idDataConnector": 1,
                "functionName": "Handle",
                "topic": "persistent://public/test/events",
            }
            | kw
        )
    )


@pytest.mark.parametrize(
    "kw",
    [
        {"servers": []},
        {"servers": ["pulsar://a", "pulsar://b"]},
        {"servers": ["http://a"]},
        {"servers": ["pulsar://user:secret@a"]},
        {"servers": ["pulsar://a/"]},
        {"servers": ["pulsar://a?"]},
        {"servers": ["pulsar://a#"]},
        {"servers": ["pulsar://a:"]},
        {"servers": ["pulsar://a:65536"]},
        {"servers": ["pulsar+ssl://a"]},
        {"adminUrl": "http://a/path"},
        {"password": "secret"},
        {"tlsServerName": "a"},
        {"credentialsFile": "file"},
        {"tlsCaFile": "ca"},
        {"operationTimeoutMs": -1},
        {"connectTimeoutMs": 2147483648},
    ],
)
def test_connector_rejections(kw):
    with pytest.raises(ValidationError) as error:
        dc(**kw)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize(
    "kw,source",
    [
        ({"topic": "public/test/events"}, True),
        ({"topic": "persistent://a/b/c/d"}, False),
        ({"partitions": 2}, False),
        ({"subscriptionName": "s", "publisherMaxInFlight": 1}, True),
        ({"subscriptionName": "s", "subscriptionInitialPosition": "Earliest"}, True),
        ({"createSubscription": False}, False),
        ({"receiverQueueSize": 1}, False),
        ({"subscriptionName": "bad/name"}, True),
        ({"streamKey": "other"}, False),
    ],
)
def test_role_rejections(kw, source):
    with pytest.raises(ValueError):
        ep(**kw).validate_role(source)


def test_defaults_projection():
    c = dc(token="secret", connectTimeoutMs=0, operationTimeoutMs=0)
    assert c.connect_timeout == 2 and c.operation_timeout == 5
    projected = data_connector_config_to_api(c)
    assert projected.admin_url == c.admin_url and projected.token is None
    e = ep(subscriptionName="s", createSubscription=False, createTopic=False)
    e.validate_role(True)
    a = endpoint_config_to_api(e)
    assert a.create_topic is False and a.create_subscription is False
    assert endpoint_config_to_api(ep()).create_subscription is None


def test_api_yaml_preserves_false_absent_and_admin_fields():
    import yaml

    from pyservicelib_gorundebug.api.models.stream_app import StreamApp
    from pyservicelib_gorundebug.runtime.config.app_to_yaml import app_to_yaml

    connector = data_connector_config_to_api(dc(token="private-token"))
    endpoint = endpoint_config_to_api(
        ep(createTopic=False, subscriptionName="workers", createSubscription=False)
    )
    app = StreamApp(
        settings={"name": "app"},
        streams=[],
        services=[],
        links=[],
        types=[],
        pools=[],
        dataConnectors=[connector],
        endpoints=[endpoint],
    )
    rendered = app_to_yaml(app)
    if isinstance(rendered, bytes):
        rendered = rendered.decode()
    assert "private-token" not in rendered
    doc = yaml.safe_load(rendered)
    entry = next(iter(doc["dataConnectors"].values()))
    assert entry["adminUrl"] == "http://localhost:18080"
    endpoint = next(iter(entry["endpoints"].values()))
    assert endpoint["createTopic"] is False and endpoint["createSubscription"] is False
    assert "subscriptionInitialPosition" not in endpoint


def test_admin_token_runtime_only():
    c = dc(token="native-token", adminToken="admin-secret")
    assert c.admin_token == "admin-secret"
    assert "admin-secret" not in repr(c)
    assert "adminToken" not in c.model_dump(by_alias=True)
    assert "admin-secret" not in str(data_connector_config_to_api(c))
    for kwargs in (
        {"adminToken": "private\r\nheader"},
        {
            "adminToken": "secret",
            "servers": ["pulsar+ssl://broker"],
            "adminUrl": "https://broker",
            "tlsCertFile": "cert",
            "tlsKeyFile": "key",
        },
    ):
        with pytest.raises(ValueError):
            dc(**kwargs)


@pytest.mark.parametrize("value", [False, True])
def test_api_projection_preserves_explicit_enable_and_tracing_flags(value):
    projected = endpoint_config_to_api(ep(enabled=value, tracingEnabled=value))
    assert projected.enabled is value
    assert projected.tracing_enabled is value
    serialized = projected.to_dict()
    assert serialized["enabled"] is value
    assert serialized["tracingEnabled"] is value
    restored = type(projected).from_json(projected.to_json())
    assert restored.enabled is value and restored.tracing_enabled is value


def test_api_projection_does_not_invent_absent_enable_and_tracing_flags():
    projected = endpoint_config_to_api(ep())
    assert projected.enabled is None and projected.tracing_enabled is None
    assert "enabled" not in projected.to_dict()
    assert "tracingEnabled" not in projected.to_dict()

import asyncio
import pytest
from pydantic import ValidationError
from pyservicelib_gorundebug.runtime.config import (
    RabbitMQDataConnectorConfig,
    RabbitMQEndpointConfig,
)
from pyservicelib_gorundebug.runtime.config.config_to_api import (
    data_connector_config_to_api,
    endpoint_config_to_api,
)
from pyservicelib_gorundebug.runtime.rabbitmq import (
    validate_connection,
    validate_endpoint,
    outgoing_headers,
    incoming_headers,
    Session,
    AdmissionRejected,
    MAX_MESSAGE_BYTES,
)
from pyservicelib_gorundebug.runtime.context.request import (
    with_stream_id,
    stream_id_from_context,
    request_context_scope,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment.tracing import sampling_scope
from rabbitmq_amqp_python_client import Message


def connector(**kwargs):
    return RabbitMQDataConnectorConfig(
        id=1, name="broker", servers=["amqp://localhost:5672"], **kwargs
    )


def endpoint(**kwargs):
    return RabbitMQEndpointConfig(
        id=2,
        name="events",
        idDataConnector=1,
        functionName="Handle",
        queueName="q",
        **kwargs,
    )


@pytest.mark.parametrize(
    "url",
    [
        "amqp://user:secret@host",
        "amqp://host/",
        "amqp://host?",
        "amqp://host#",
        "amqp://host:",
        "amqp://host:0",
        "amqp://host:65536",
        "http://host",
        "amqp://bad host",
        "amqp://host\n",
    ],
)
def test_connection_urls_and_secret_safe_errors(url):
    c = connector()
    c.servers = [url]
    with pytest.raises(ValueError) as error:
        validate_connection(c)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize(
    "options",
    [
        dict(username="u"),
        dict(password="secret"),
        dict(token="secret"),
        dict(credentialsFile="/secret"),
        dict(tlsCaFile="/ca"),
        dict(tlsEnabled=True, tlsKeyFile="/key"),
        dict(tlsEnabled=True, tlsServerName="other"),
        dict(implementation="aio-pika"),
        dict(virtualHost="bad\n"),
        dict(virtualHost="has space"),
        dict(virtualHost="tenant?query"),
    ],
)
def test_invalid_auth_tls_and_implementation(options):
    with pytest.raises(ValueError):
        validate_connection(connector(**options))


@pytest.mark.parametrize(
    "options",
    [
        dict(connectTimeoutMs=0),
        dict(operationTimeoutMs=-1),
        dict(connectTimeoutMs=True),
        dict(operationTimeoutMs=9223372036855),
    ],
)
def test_timeout_bounds(options):
    with pytest.raises(ValueError):
        connector(**options)


@pytest.mark.parametrize(
    "options",
    [
        dict(createBinding=True),
        dict(createExchange=True),
        dict(routingKey="r"),
        dict(exchangeName="x", exchangeType="direct"),
        dict(publisherMaxInFlight=1),
        dict(queueName=None),
    ],
)
def test_source_settings(options):
    q = options.pop("queueName", "q")
    e = endpoint(**options)
    e.queue_name = q
    with pytest.raises(ValueError):
        validate_endpoint(e, source=True)


@pytest.mark.parametrize("value", [0, -1, 4097, True])
def test_admission_limit_bounds(value):
    with pytest.raises(ValueError):
        endpoint(consumerCredits=value)


def test_runtime_projection_preserves_flags_and_native_fields():
    c = connector(virtualHost="tenant", tlsEnabled=False)
    validate_connection(c)
    a = data_connector_config_to_api(c)
    assert (
        a.virtual_host == "tenant"
        and a.tls_enabled is False
        and a.operation_timeout_ms == 5000
    )
    a.servers.append("changed")
    assert c.servers == ["amqp://localhost:5672"]
    e = endpoint(
        enabled=False,
        tracingEnabled=False,
        createQueue=False,
        createExchange=False,
        createBinding=False,
        routingKey="",
        consumerCredits=7,
    )
    a = endpoint_config_to_api(e)
    assert (
        a.create_queue is False
        and a.enabled is False
        and a.tracing_enabled is False
        and a.routing_key == ""
        and a.consumer_credits == 7
    )
    assert a.publisher_max_in_flight is None
    a = endpoint_config_to_api(endpoint())
    assert (
        a.create_queue is None
        and a.create_exchange is None
        and a.create_binding is None
    )
    assert a.consumer_credits is None and a.publisher_max_in_flight is None


def test_case_insensitive_trace_propagation_keeps_native_properties():
    original = {
        "X-Stream-ID": "old",
        "X-TrAcE": "1",
        "application": 17,
        "TraceParent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
    }
    with request_context_scope(Context()), incoming_headers(original) as carrier:
        assert carrier["x-stream-id"] == "old"
        with_stream_id("request")
        result = outgoing_headers(original)
    assert result["x-stream-id"] == "request" and "X-Stream-ID" not in result
    assert result["application"] == 17 and result["x-trace"] == "1"
    assert original["X-Stream-ID"] == "old"

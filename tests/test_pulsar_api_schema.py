"""Pulsar schema bindings only; these tests do not enable a runtime adapter."""

import pytest
from pydantic import ValidationError

from pyservicelib_gorundebug.api.models.data_connector import DataConnector
from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.endpoint import Endpoint


def test_pulsar_binding_ids_implementations_and_roundtrip():
    assert [
        DataConnectorType.NATS.value,
        DataConnectorType.JetStream.value,
        DataConnectorType.RabbitMQ.value,
        DataConnectorType.RedisStreams.value,
        DataConnectorType.Pulsar.value,
    ] == [7, 8, 9, 10, 11]
    assert DataConnectorImplementation.PulsarPython.value == "pulsar-client-python"
    dc = DataConnector(
        id=1,
        name="Pulsar",
        type=DataConnectorType.Pulsar,
        implementation=DataConnectorImplementation.PulsarPython,
        servers=["pulsar://broker:6650"],
        adminUrl="http://broker:8080",
    )
    assert DataConnector.model_validate_json(dc.model_dump_json(by_alias=True)) == dc
    ep = Endpoint(
        id=2,
        name="Receive",
        idDataConnector=1,
        functionName="Receive",
        topic="persistent://public/default/events",
        subscriptionName="workers",
        subscriptionType="Shared",
        createSubscription=False,
        subscriptionInitialPosition="Earliest",
        receiverQueueSize=32,
        negativeAckRedeliveryDelayMs=1000,
        createTopic=False,
        partitions=1,
        publisherMaxInFlight=16,
    )
    restored = Endpoint.model_validate_json(ep.model_dump_json(by_alias=True))
    assert restored == ep and restored.create_subscription is False
    assert (
        restored.receiver_queue_size == 32
        and restored.negative_ack_redelivery_delay_ms == 1000
    )
    absent = Endpoint(id=3, name="Draft", idDataConnector=1, functionName="Receive")
    assert "createSubscription" not in absent.model_dump(
        by_alias=True, exclude_unset=True
    )
    assert (
        ep.model_dump(by_alias=True, exclude_unset=True)["createSubscription"] is False
    )


@pytest.mark.parametrize(
    "fields",
    [
        {"subscriptionType": "Invalid"},
        {"subscriptionInitialPosition": "Invalid"},
        {"receiverQueueSize": 0},
        {"receiverQueueSize": 4097},
        {"negativeAckRedeliveryDelayMs": 0},
        {"negativeAckRedeliveryDelayMs": 2147483648},
        {"createSubscription": 1},
    ],
)
def test_pulsar_api_rejects_invalid_fields(fields):
    with pytest.raises(ValidationError):
        Endpoint(
            id=2, name="Receive", idDataConnector=1, functionName="Receive", **fields
        )

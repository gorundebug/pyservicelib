"""Public generated API serialization must preserve topology field presence."""
import json

import pytest
from pydantic import ValidationError

from pyservicelib_gorundebug.api.models.data_connector import DataConnector
from pyservicelib_gorundebug.api.models.endpoint import Endpoint
from pyservicelib_gorundebug.api.models.stream_app import StreamApp


ENDPOINT = {"id": 2, "name": "Publish", "idDataConnector": 1, "functionName": "Publish"}
CONNECTOR = {"id": 1, "name": "Broker", "type": 11}
FLAGS = (
    "enabled", "tracingEnabled", "createStream", "createConsumer", "createQueue",
    "createExchange", "createBinding", "createConsumerGroup", "createTopic",
    "createSubscription",
)


@pytest.mark.parametrize("model,document", [(Endpoint, ENDPOINT), (DataConnector, CONNECTOR)])
@pytest.mark.parametrize("factory", ["constructor", "from_dict", "from_json"])
def test_omitted_fields_stay_omitted(model, document, factory):
    if factory == "constructor":
        value = model(**document)
    elif factory == "from_dict":
        value = model.from_dict(document)
    else:
        value = model.from_json(json.dumps(document))
    assert value.to_dict() == document
    assert json.loads(value.to_json()) == document
    assert value.model_dump(by_alias=True, exclude_unset=True) == document
    assert model.from_dict(value.to_dict()).to_dict() == document


@pytest.mark.parametrize("flag", FLAGS)
@pytest.mark.parametrize("value", [False, True])
def test_explicit_endpoint_flags_survive_public_roundtrip(flag, value):
    document = ENDPOINT | {flag: value}
    endpoint = Endpoint.from_dict(document)
    assert endpoint.to_dict() == document
    assert Endpoint.from_json(endpoint.to_json()).to_dict() == document


@pytest.mark.parametrize("field,value", [("tlsEnabled", False), ("redisDatabase", 0), ("connectTimeoutMs", 1)])
def test_explicit_connector_false_and_zero_survive(field, value):
    document = CONNECTOR | {field: value}
    assert DataConnector.from_json(DataConnector.from_dict(document).to_json()).to_dict() == document


def test_explicit_null_does_not_turn_into_schema_default():
    endpoint = Endpoint.from_dict(ENDPOINT | {"createSubscription": None})
    assert endpoint.create_subscription is None
    assert "createSubscription" not in endpoint.to_dict()  # schema is non-nullable


@pytest.mark.parametrize("flag", FLAGS)
def test_presence_fix_does_not_weaken_boolean_validation(flag):
    with pytest.raises(ValidationError):
        Endpoint.from_dict(ENDPOINT | {flag: 0})


def test_nested_stream_app_roundtrip_preserves_endpoint_and_connector_presence():
    document = {
        "settings": {"name": "Presence"},
        "streams": [], "services": [], "links": [], "types": [], "pools": [],
        "dataConnectors": [CONNECTOR | {"tlsEnabled": False}],
        "endpoints": [ENDPOINT, ENDPOINT | {"id": 3, "createSubscription": False}],
    }
    result = StreamApp.from_json(StreamApp.from_dict(document).to_json()).to_dict()
    assert result["dataConnectors"] == document["dataConnectors"]
    assert result["endpoints"] == document["endpoints"]

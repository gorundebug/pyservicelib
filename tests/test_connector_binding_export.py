from typing import Any

import pytest
import yaml

from pyservicelib_gorundebug.api.models.data_connector import DataConnector
from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
from pyservicelib_gorundebug.api.models.stream_app import StreamApp
from pyservicelib_gorundebug.runtime.config.app_to_yaml import app_to_yaml
from pyservicelib_gorundebug.runtime.config.config import DataConnectorConfig
from pyservicelib_gorundebug.runtime.config.config_to_api import (
    data_connector_config_to_api,
)


def exported_connector(connector: DataConnector) -> dict[str, Any]:
    app = StreamApp(
        settings=ProjectSettings(name="Test"),
        services=[],
        streams=[],
        links=[],
        types=[],
        dataConnectors=[connector],
        endpoints=[],
        pools=[],
    )
    document: dict[str, Any] = yaml.safe_load(app_to_yaml(app))
    result: dict[str, Any] = document["dataConnectors"]["transport"]
    return result


@pytest.mark.parametrize(
    ("kind", "implementation"),
    [
        (DataConnectorType.HTTP, DataConnectorImplementation.AioHTTP),
        (DataConnectorType.gRPC, DataConnectorImplementation.GoogleGRPC),
        (DataConnectorType.Kafka, DataConnectorImplementation.AioKafka),
        (DataConnectorType.Cron, DataConnectorImplementation.PythonAPScheduler),
        (DataConnectorType.Temporal, DataConnectorImplementation.TemporalPython),
    ],
)
def test_runtime_exports_selected_python_transport(
    kind: DataConnectorType, implementation: DataConnectorImplementation
) -> None:
    config = DataConnectorConfig(
        id=1, name="Transport", type=kind, implementation=implementation
    )
    api = data_connector_config_to_api(config)
    assert api.implementation is None
    assert api.implementations == {"python": implementation.value}
    output = exported_connector(api)
    assert output["implementations"] == {"python": implementation.value}
    assert "implementation" not in output
    assert config.implementations is None


def test_runtime_keeps_custom_connector_singleton() -> None:
    config = DataConnectorConfig(
        id=1,
        name="Transport",
        type=DataConnectorType.Custom,
        implementation=DataConnectorImplementation.Function,
    )
    api = data_connector_config_to_api(config)
    assert api.implementation is DataConnectorImplementation.Function
    assert api.implementations is None
    output = exported_connector(api)
    assert output["implementation"] == "function"
    assert "implementations" not in output


def test_runtime_selected_transport_overrides_authored_python_without_mutation() -> None:
    authored = {"python": "custom/http", "external": "external/http"}
    config = DataConnectorConfig(
        id=1,
        name="Transport",
        type=DataConnectorType.HTTP,
        implementation=DataConnectorImplementation.AioHTTP,
        implementations=authored,
    )
    api = data_connector_config_to_api(config)
    assert api.implementations == {"python": "aiohttp", "external": "external/http"}
    assert config.implementations == authored
    assert api.implementations is not config.implementations


@pytest.mark.parametrize("bindings", [None, {}, {"external": "custom/http"}])
def test_api_and_runtime_export_optional_open_bindings(
    bindings: dict[str, str] | None,
) -> None:
    api = DataConnector(
        id=1, name="Transport", type=DataConnectorType.HTTP, implementations=bindings
    )
    restored = DataConnector.from_json(api.to_json())
    assert restored is not None
    assert restored.implementations == bindings
    runtime_api = data_connector_config_to_api(
        DataConnectorConfig(
            id=1,
            name="Transport",
            type=DataConnectorType.HTTP,
            implementations=bindings,
        )
    )
    for connector in (restored, runtime_api):
        output = exported_connector(connector)
        if bindings is None:
            assert "implementations" not in output
        else:
            assert output["implementations"] == bindings
        assert "implementation" not in output

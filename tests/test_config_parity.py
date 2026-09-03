import pytest
import warnings
import yaml

from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.api.models.data_connector import DataConnector
from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.endpoint import Endpoint
from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
from pyservicelib_gorundebug.api.models.stream_app import StreamApp
from pyservicelib_gorundebug.runtime.config.app_to_yaml import app_to_yaml
from pyservicelib_gorundebug.runtime.config.config import (
    DataConnectorConfig,
    ModuleConfig,
)
from pyservicelib_gorundebug.runtime.config.config_to_api import (
    data_connector_config_to_api,
)
from pyservicelib_gorundebug.runtime.config.dataconnector_types import (
    GrpcDataConnectorConfig,
)


def test_module_config_accepts_go_runtime_path_shape() -> None:
    config = ModuleConfig.from_dict({"name": "shared", "path": "example/shared"})

    assert config is not None
    assert config.name == "shared"
    assert config.module_path == "example/shared"
    assert config.golang_version == ""


def test_grpc_connections_count_defaults_and_validates() -> None:
    implementation = DataConnectorImplementation.GoogleGRPC
    config = GrpcDataConnectorConfig(1, "inventory", implementation)
    assert config.connections_count == 1
    assert config.to_dict()["connectionsCount"] == 1

    with pytest.raises(ValueError, match="at least 1"):
        GrpcDataConnectorConfig(
            1,
            "inventory",
            implementation,
            connections_count=0,
        )


def test_api_connections_count_is_not_defaulted_for_every_connector() -> None:
    http = DataConnector(id=1, name="http", type=DataConnectorType.HTTP)
    grpc = DataConnector(id=2, name="grpc", type=DataConnectorType.gRPC)

    assert "connectionsCount" not in http.to_dict()
    assert "connectionsCount" not in grpc.to_dict()
    assert DataConnector(
        id=2,
        name="grpc",
        type=DataConnectorType.gRPC,
        connectionsCount=1,
    ).to_dict()["connectionsCount"] == 1

    http_config = DataConnectorConfig(
        id=1,
        name="http",
        type=DataConnectorType.HTTP,
        implementation="net/http",
    )
    grpc_config = DataConnectorConfig(
        id=2,
        name="grpc",
        type=DataConnectorType.gRPC,
        implementation="google/grpc",
    )
    assert "connectionsCount" not in data_connector_config_to_api(http_config).to_dict()
    assert data_connector_config_to_api(grpc_config).to_dict()["connectionsCount"] == 1


def test_runtime_connector_implementation_serializes_as_a_typed_enum() -> None:
    config = DataConnectorConfig(
        id=1,
        name="temporal",
        type=DataConnectorType.Temporal,
        implementation="temporal/python",
    )

    assert config.implementation is DataConnectorImplementation.TemporalPython
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert config.model_dump(mode="json")["implementation"] == "temporal/python"


def test_non_temporal_endpoint_yaml_does_not_serialize_absent_temporal_fields() -> None:
    app = StreamApp(
        settings=ProjectSettings(name="test"),
        streams=[],
        services=[],
        links=[],
        types=[],
        dataConnectors=[
            DataConnector(id=1, name="HTTP", type=DataConnectorType.HTTP)
        ],
        endpoints=[
            Endpoint(
                id=1,
                name="Endpoint",
                idDataConnector=1,
                functionName="EndpointSource",
            )
        ],
        pools=[],
    )

    document = yaml.safe_load(app_to_yaml(app))
    endpoint = document["dataConnectors"]["http"]["endpoints"]["endpoint"]

    assert endpoint == {
        "name": "Endpoint",
        "functionName": "EndpointSource",
    }

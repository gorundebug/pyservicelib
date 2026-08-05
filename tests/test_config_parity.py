import pytest

from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.runtime.config.config import ModuleConfig
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

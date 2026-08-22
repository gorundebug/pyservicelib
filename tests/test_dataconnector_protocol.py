from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.runtime.config import data_connector_protocol


def test_data_connector_protocol() -> None:
    assert data_connector_protocol(DataConnectorType.Undefined) is None
    assert data_connector_protocol(DataConnectorType.HTTP) is None
    assert data_connector_protocol(DataConnectorType.gRPC) == "grpc"
    assert data_connector_protocol(DataConnectorType.Kafka) is None
    assert data_connector_protocol(DataConnectorType.Custom) is None

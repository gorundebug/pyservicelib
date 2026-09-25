from pyservicelib_gorundebug.runtime.config.config import EndpointConfig
from pyservicelib_gorundebug.runtime.environment.tracing import data_source_endpoint_tracing_enabled


class _ReloadableConfig:
    def __init__(self) -> None:
        self.endpoint: EndpointConfig = EndpointConfig(id=100, name="Route", idDataConnector=1, functionName="Source")

    def get_endpoint_config_by_id(self, endpoint_id: int) -> EndpointConfig:
        assert endpoint_id == 100
        return self.endpoint


class _Environment:
    def __init__(self, config: _ReloadableConfig) -> None:
        self._config = config

    @property
    def config(self) -> _ReloadableConfig:
        return self._config


def test_data_source_endpoint_tracing_reads_current_config_snapshot() -> None:
    config = _ReloadableConfig()
    environment = _Environment(config)
    assert not data_source_endpoint_tracing_enabled(environment, 100)
    config.endpoint = EndpointConfig(id=100, name="Route", idDataConnector=1, functionName="Source", tracingEnabled=True)
    assert data_source_endpoint_tracing_enabled(environment, 100)
    config.endpoint = EndpointConfig(id=100, name="Route", idDataConnector=1, functionName="Source", tracingEnabled=False)
    assert not data_source_endpoint_tracing_enabled(environment, 100)

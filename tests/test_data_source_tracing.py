from types import SimpleNamespace

from pyservicelib_gorundebug.runtime.environment.tracing import (
    data_source_endpoint_tracing_enabled,
)


class _ReloadableConfig:
    def __init__(self) -> None:
        self.endpoint = SimpleNamespace(tracing_enabled=False)

    def get_endpoint_config_by_id(self, endpoint_id: int) -> object:
        assert endpoint_id == 100
        return self.endpoint


def test_data_source_endpoint_tracing_reads_current_config_snapshot() -> None:
    config = _ReloadableConfig()
    environment = SimpleNamespace(config=config)

    assert not data_source_endpoint_tracing_enabled(environment, 100)
    config.endpoint = SimpleNamespace(tracing_enabled=True)
    assert data_source_endpoint_tracing_enabled(environment, 100)
    config.endpoint = SimpleNamespace(tracing_enabled=False)
    assert not data_source_endpoint_tracing_enabled(environment, 100)

import pytest

from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.runtime.common import ServiceExecutionEnvironment, Stream
from pyservicelib_gorundebug.runtime.config import StreamConfig
from pyservicelib_gorundebug.runtime.stream_grouping import stream_grouping


class ConfiguredStream(Stream):
    def __init__(self, config: StreamConfig) -> None:
        self._config = config
        self.config_reads = 0

    @property
    def config(self) -> StreamConfig:
        self.config_reads += 1
        return self._config

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def id(self) -> int:
        return self._config.id

    @property
    def transformation_name(self) -> str:
        return self._config.transformation_name

    @property
    def type_name(self) -> str:
        return "Amount"

    @property
    def consumers(self) -> list[Stream]:
        return []

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        raise AssertionError("Grouping must not inspect the environment")


@pytest.mark.parametrize("pipeline", [None, "", "pricing"])
@pytest.mark.parametrize("component", [None, "", "Customer Pricing"])
def test_grouping_uses_typed_config_once(pipeline: str | None, component: str | None) -> None:
    stream = ConfiguredStream(StreamConfig(
        id=2, name="Calculate Price", idSource=1, type=TransformationType.Map, idService=1,
        xPos=0, yPos=0, valueType="Amount", pipeline=pipeline, component=component,
    ))

    assert stream_grouping(stream) == (pipeline or "", component or "")
    assert stream.config_reads == 1


def test_missing_configuration_is_not_silently_treated_as_empty_grouping() -> None:
    class BrokenConfiguredStream(ConfiguredStream):
        @property
        def config(self) -> StreamConfig:
            raise AttributeError("Missing stream configuration")

    stream = BrokenConfiguredStream(StreamConfig(
        id=2, name="Calculate Price", idSource=1, type=TransformationType.Map, idService=1,
        xPos=0, yPos=0, valueType="Amount",
    ))
    with pytest.raises(AttributeError, match="Missing stream configuration"):
        stream_grouping(stream)

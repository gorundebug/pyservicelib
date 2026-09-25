from typing import cast
from pyservicelib_gorundebug.runtime.common import ServiceExecutionEnvironment, TypedStream
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.api.models.stream import Stream
from pyservicelib_gorundebug.runtime.common import RuntimeHelpers
from pyservicelib_gorundebug.runtime.config.config import (
    RuntimeConfig, ServiceAppConfig, StreamConfig,
)
from pyservicelib_gorundebug.runtime.testmetrics.testmetrics import TestMetrics as Metrics


@pytest.mark.parametrize("owner_type", [
    TransformationType.Input, TransformationType.Process, TransformationType.Sink,
])
def test_error_link_reference_resolves_owner_config(owner_type):
    owner = StreamConfig(id=1, name="Owner", type=owner_type, idService=1, idSource=0, xPos=0, yPos=0)
    runtime = RuntimeConfig()
    runtime.streams_by_id[1] = owner
    config = ServiceAppConfig.model_construct(runtime_config=runtime)
    assert config.get_stream_config_by_id(-1) is owner
    assert config.get_stream_config_by_id(1) is owner
    with pytest.raises(KeyError):
        config.get_stream_config_by_id(-2)


def test_negative_reference_does_not_create_an_error_output_on_map():
    runtime = RuntimeConfig()
    runtime.streams_by_id[1] = StreamConfig(
        id=1, name="Map", type=TransformationType.Map, idService=1, idSource=0, xPos=0, yPos=0,
    )
    config = ServiceAppConfig.model_construct(runtime_config=runtime)
    with pytest.raises(KeyError):
        config.get_stream_config_by_id(-1)


@pytest.mark.asyncio
async def test_normal_and_error_outputs_to_same_target_use_independent_links():
    received, pool_calls, lookups = [], [], []

    async def consume(value):
        received.append(value)

    class Pool:
        name = "Recovery"

        async def add_task(self, *args):
            pool_calls.append(self.name)
            await args[-1]()

    links = {
        (1, 2): SimpleNamespace(call_semantics=CallSemantics.FunctionCall, var_async=False),
        (-1, 2): SimpleNamespace(call_semantics=CallSemantics.TaskPool, pool_name="Recovery"),
    }

    def get_link(source_id, target_id):
        lookups.append((source_id, target_id))
        return links.get((source_id, target_id))

    runtime = SimpleNamespace(
        register_consume_statistics=lambda *args: None,
        register_link_info=lambda *args: None,
        get_task_pool=lambda name: Pool(),
    )
    environment = SimpleNamespace(
        config=SimpleNamespace(get_link=get_link), runtime=runtime, metrics=Metrics(), tracing=None,
        service_config=SimpleNamespace(id=1, name="Service", default_call_semantics=CallSemantics.FunctionCall),
    )
    target = SimpleNamespace(id=2, name="Target", config=SimpleNamespace(pipeline="main", component=""))
    consumer = SimpleNamespace(stream=target, consume=consume)
    for error_output, value in [(False, "result"), (True, "failure")]:
        source = SimpleNamespace(
            id=1, name="Owner", is_error_stream=error_output,
            consumer=consumer, environment=environment, config=SimpleNamespace(id_service=1),
        )
        await RuntimeHelpers[str](cast(ServiceExecutionEnvironment, environment)).make_caller(cast(TypedStream[str], source)).consume(value)
        assert source.id == 1
    assert lookups == [(1, 2), (-1, 2)]
    assert received == ["result", "failure"]
    assert pool_calls == ["Recovery"]


@pytest.mark.parametrize("references", [{"idSource": -1}, {"idSource": 0, "idSources": [1, -1]}])
def test_runtime_signed_references_do_not_weaken_graph_model(references):
    data = dict(id=2, name="Target", type=TransformationType.Merge, idService=1,
                xPos=0, yPos=0, **references)
    runtime = StreamConfig.model_validate(data)
    assert runtime.model_dump(by_alias=True)["idSource"] == references["idSource"]
    with pytest.raises(ValidationError):
        Stream.model_validate(data)


@pytest.mark.parametrize("references", [
    {"idSource": "-1"}, {"idSource": -1.0}, {"idSource": True},
    {"idSource": 0, "idSources": [0]}, {"idSource": 0, "idSources": ["-1"]},
])
def test_runtime_references_remain_strict_integers(references):
    with pytest.raises(ValidationError):
        StreamConfig.model_validate(dict(id=2, name="Target", type=TransformationType.Merge,
                                        idService=1, xPos=0, yPos=0, **references))

"""In-process graph edges preserve values and do not invoke wire serializers."""

from types import SimpleNamespace
from typing import Any, Never, cast

import pytest

from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.operators.filter import FilterStream
from pyservicelib_gorundebug.operators.functions import FilterFunction, MapFunction
from pyservicelib_gorundebug.operators.map import MapStream
from pyservicelib_gorundebug.operators.merge import MergeStream
from pyservicelib_gorundebug.operators.split import SplitStream
from pyservicelib_gorundebug.runtime.common import (
    Collect, ServiceExecutionEnvironment, TypedConsumedStream, Stream,
)
from pyservicelib_gorundebug.runtime.config import StreamConfig
from pyservicelib_gorundebug.runtime.config.stream_types import (
    FilterStreamConfig, MapStreamConfig, MergeStreamConfig, SplitStreamConfig,
)
from pyservicelib_gorundebug.runtime.serde import Serde, StreamSerde


class _NoWireSerialization:
    def serialize(self, *args: Any) -> Never:
        raise AssertionError("in-process edge serialized the value")

    def deserialize(self, *args: Any) -> Never:
        raise AssertionError("in-process edge deserialized the value")


class _Entry(TypedConsumedStream[object]):
    async def consume(self, value: object) -> None:
        assert self._caller is not None
        await self._caller.consume(value)


class _Accept(FilterFunction[object]):
    async def filter(self, stream: Stream, value: object) -> bool:
        return True


class _Identity(MapFunction[object, object]):
    async def map(self, stream: Stream, value: object, out: Collect[object]) -> None:
        await out.out(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("async_link", [False, True])
async def test_direct_pipeline_inherits_serde_without_serializing(async_link: bool) -> None:
    configurations = {
        stream_id: StreamConfig(
            id=stream_id, name=f"node-{stream_id}", idSource=0,
            type=kind, idService=1, xPos=0, yPos=0, valueType="Output",
        )
        for stream_id, kind in enumerate((
            TransformationType.Input, TransformationType.Split,
            TransformationType.Filter, TransformationType.Filter,
            TransformationType.Merge, TransformationType.Map,
            TransformationType.Map,
        ), start=1)
    }
    inherited = StreamSerde(cast(Serde[object], _NoWireSerialization()))
    transformed = StreamSerde(cast(Serde[object], _NoWireSerialization()))
    lookups: list[str] = []

    def get_registered_serde(type_name: str):
        lookups.append(type_name)
        assert type_name == "Output"
        return transformed

    def reject_type_lookup(type_name: str) -> Never:
        raise AssertionError(f"registered serde ignored for {type_name}")

    env = cast(ServiceExecutionEnvironment, SimpleNamespace(
        tracing=None, metrics=SimpleNamespace(enabled=False),
        service_config=SimpleNamespace(id=1, name="serde-test", default_call_semantics=CallSemantics.FunctionCall),
        config=SimpleNamespace(
            get_stream_config_by_id=configurations.__getitem__,
            get_link=lambda _source, _target: SimpleNamespace(
                call_semantics=CallSemantics.FunctionCall, var_async=async_link,
            ),
        ),
        runtime=SimpleNamespace(
            register_stream=lambda _stream: None,
            register_consume_statistics=lambda *_args: None,
            register_link_info=lambda _link: None,
            get_registered_serde=get_registered_serde,
            get_type_serde=reject_type_lookup,
        ),
    ))
    entry = _Entry(1, env, inherited)
    split = SplitStream(SplitStreamConfig(configurations[2]), entry)
    first, second = split.add_stream(), split.add_stream()
    left = FilterStream(FilterStreamConfig(configurations[3]), first, _Accept())
    right = FilterStream(FilterStreamConfig(configurations[4]), second, _Accept())
    merge = MergeStream(MergeStreamConfig(configurations[5]), left, right)
    mapped = MapStream(MapStreamConfig(configurations[6]), merge, _Identity())
    observed: list[object] = []

    class Terminal(TypedConsumedStream[object]):
        async def consume(self, value: object) -> None:
            observed.append(value)

    terminal = Terminal(7, env, transformed)
    mapped.consumer = terminal
    split.build()

    for node in (entry, split, first, second, left, right, merge):
        assert node.serde is inherited
    assert mapped.serde is transformed
    assert lookups == ["Output"]

    value = {"large-payload": bytearray(256 * 1024)}
    await entry.consume(value)
    assert len(observed) == 2
    assert all(result is value for result in observed)

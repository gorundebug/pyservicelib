"""Case matches Go routing without negative-index wraparound or wire conversion."""

from collections.abc import Callable
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, Never, cast
from unittest.mock import Mock

import pytest

from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.operators.case import CaseStream, WhenStream
from pyservicelib_gorundebug.operators.functions import When
from pyservicelib_gorundebug.runtime.common import (
    ServiceExecutionEnvironment, Stream, TypedConsumedStream,
)
from pyservicelib_gorundebug.runtime.config import StreamConfig
from pyservicelib_gorundebug.runtime.config.stream_types import CaseStreamConfig, WhenStreamConfig
from pyservicelib_gorundebug.runtime.environment.tracing import Tracer
from pyservicelib_gorundebug.runtime.serde import Serde, StreamSerde


class _NoWire:
    def serialize(self, *args: Any) -> Never:
        raise AssertionError("Case serialized its payload")

    def deserialize(self, *args: Any) -> Never:
        raise AssertionError("Case deserialized its payload")


class _Switch:
    index = 0

    def build_switch(self, stream: Stream, when_items: list[When]) -> Callable[[object], int]:
        return lambda value: self.index


class _Entry(TypedConsumedStream[object]):
    async def consume(self, value: object) -> None:
        assert self._caller is not None
        await self._caller.consume(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("traced", [False, True])
async def test_case_serde_identity_and_invalid_indices(
    traced: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configs = {
        index: StreamConfig(
            id=index, name=f"node-{index}", idSource=0, type=kind,
            idService=1, xPos=0, yPos=0, valueType="Output",
        )
        for index, kind in enumerate((
            TransformationType.Input, TransformationType.Case,
            TransformationType.When, TransformationType.When,
            TransformationType.Map, TransformationType.Map,
        ), start=1)
    }
    parent_serde = StreamSerde(cast(Serde[object], _NoWire()))
    branch_serde = StreamSerde(cast(Serde[object], _NoWire()))
    lookups: list[str] = []

    def get_serde(name: str) -> StreamSerde[object]:
        lookups.append(name)
        assert name == "Output"
        return branch_serde

    env = cast(ServiceExecutionEnvironment, SimpleNamespace(
        tracing=None, metrics=SimpleNamespace(enabled=False),
        service_config=SimpleNamespace(
            id=1, name="case-test", default_call_semantics=CallSemantics.FunctionCall,
        ),
        config=SimpleNamespace(
            get_stream_config_by_id=configs.__getitem__,
            get_link=lambda *_args: SimpleNamespace(
                call_semantics=CallSemantics.FunctionCall, var_async=False,
            ),
        ),
        runtime=SimpleNamespace(
            register_stream=lambda _stream: None,
            register_consume_statistics=lambda *_args: None,
            register_link_info=lambda _link: None,
            get_registered_serde=get_serde,
        ),
    ))
    selector = _Switch()
    source = _Entry(1, env, parent_serde)
    case = CaseStream(CaseStreamConfig(configs[2]), source, selector)
    branches = [WhenStream[object, object](WhenStreamConfig(configs[i]), case) for i in (3, 4)]
    received: list[tuple[int, object]] = []

    class Terminal(TypedConsumedStream[object]):
        async def consume(self, value: object) -> None:
            received.append((self.id, value))

    for index, branch in enumerate(branches, start=5):
        branch.consumer = Terminal(index, env, branch_serde)
        assert branch.serde is branch_serde
    assert case.serde is parent_serde
    assert lookups == ["Output", "Output"]
    span = Mock()
    span.scoped.side_effect = nullcontext
    if traced:
        case._tracer = cast(Tracer, Mock())
        monkeypatch.setattr("pyservicelib_gorundebug.operators.case.sampling_enabled", lambda: True)
        monkeypatch.setattr(
            "pyservicelib_gorundebug.operators.case.start_stream_span",
            lambda *_args: (None, span),
        )

    value = {"payload": bytearray(256 * 1024)}
    with pytest.raises(RuntimeError, match="not built"):
        await case.consume(value)
    case.build()
    for index in (0, 1):
        selector.index = index
        await case.consume(value)
    assert [index for index, _ in received] == [5, 6]
    assert all(output is value for _, output in received)
    for index in (-1, -2, -3, 2, 100):
        selector.index = index
        with pytest.raises(IndexError, match="only 2 branches exist"):
            await case.consume(value)
    assert len(received) == 2
    if traced:
        assert span.end.call_count == 8

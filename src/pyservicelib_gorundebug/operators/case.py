#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Any, Optional, Callable, cast

from ..runtime.common import (
    TypedConsumedStream, TypedCaseStream, TypedWhenStream, TypedStream, Stream,
    RuntimeHelpers,
)
from ..runtime.config.stream_types import CaseStreamConfig, WhenStreamConfig
from .functions import BuildSwitchFunction, When


class WhenStream[T, R](TypedConsumedStream[R], TypedWhenStream):
    """
    One branch of a CaseStream switch. Receives values of type T from CaseStream
    (via consume_case), casts them to R, and routes downstream.

    Equivalent to Go's WhenStream[T, R] which embeds streamLink (identity delegates
    to caseStream) and implements TypedWhenStream[R].
    """
    _case_stream: "CaseStream[T]"
    _index: int

    def __init__(self, cfg: WhenStreamConfig, case_stream: "CaseStream[T]"):
        serde = RuntimeHelpers[R](case_stream.environment).make_stream_serde(type_name=cfg.value_type)
        super().__init__(stream_id=cfg.id, env=case_stream.environment, serde=serde)
        self._case_stream = case_stream
        self._index = 0
        case_stream.add_stream(self)

    def set_index(self, index: int) -> None:
        self._index = index

    async def consume(self, value: R) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    async def consume_case(self, value: Any) -> None:
        await self.consume(cast(R, value))

    def get_when_consumer(self) -> Stream:
        if self._consumer is not None:
            return self._consumer.stream
        return self

    @property
    def type(self) -> type:
        try:
            return self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
        except AttributeError:
            raise RuntimeError(
                "WhenStream.type requires explicit type annotation: WhenStream[T, R](...)"
            )


class CaseStream[T](TypedCaseStream[T]):
    """
    Routes each T value to one of several WhenStream branches based on a switch function.
    Equivalent to Go's CaseStream[T].
    """
    _source: TypedStream[T]
    _when_streams: list[TypedWhenStream]
    _build_switch: BuildSwitchFunction[T]
    _when_func: Optional[Callable[[T], int]]

    def __init__(self, cfg: CaseStreamConfig, stream: TypedStream[T],
                 build_switch: BuildSwitchFunction[T]):
        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)
        self._source = stream
        self._when_streams = []
        self._build_switch = build_switch
        self._when_func = None
        stream.consumer = self

    def add_stream(self, stream: TypedWhenStream) -> None:
        stream.set_index(len(self._when_streams))
        self._when_streams.append(stream)

    async def consume(self, value: T) -> None:
        if self._when_func is not None:
            index = self._when_func(value)
            await self._when_streams[index].consume_case(value)

    def build(self) -> None:
        self._when_func = self._build_switch.build_switch(self, self._when_streams)  # type: ignore[arg-type]

    @property
    def consumers(self) -> list[Stream]:
        return self._when_streams  # type: ignore[return-value]

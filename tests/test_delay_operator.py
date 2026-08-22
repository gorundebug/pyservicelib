import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

import pytest

from pyservicelib_gorundebug.operators.delay import DelayStream
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled,
    request_deadline,
)


class _Config:
    def get_stream_config_by_id(self, stream_id: int) -> Any:
        return SimpleNamespace(name=f"delay-{stream_id}")


class _Environment:
    config = _Config()

    async def delay(
        self,
        duration: timedelta,
        task: Callable[[int], Awaitable[None]],
        value: int,
    ) -> None:
        await asyncio.sleep(duration.total_seconds())
        await task(value)


class _RejectingEnvironment(_Environment):
    async def delay(
        self,
        duration: timedelta,
        task: Callable[[int], Awaitable[None]],
        value: int,
    ) -> None:
        del duration, task, value
        raise RuntimeError("request cancelled")


class _DelayFunctionContext:
    def __init__(self, duration: timedelta) -> None:
        self.duration = duration
        self.errors: list[Exception] = []

    async def call(self, value: int) -> timedelta:
        del value
        return self.duration

    async def call_error(
        self,
        value: int,
        error: Exception,
        out: Any,
    ) -> None:
        del value, out
        self.errors.append(error)


class _Caller:
    def __init__(self) -> None:
        self.values: list[int] = []

    async def consume(self, value: int) -> None:
        self.values.append(value)


def _make_stream(
    environment: _Environment,
    duration: timedelta,
) -> tuple[DelayStream[int], _Caller, _DelayFunctionContext]:
    stream = object.__new__(DelayStream)
    stream_state: Any = stream
    caller = _Caller()
    function = _DelayFunctionContext(duration)
    stream_state._id = 1
    stream_state._environment = environment
    stream_state._tracer = None
    stream_state._caller = caller
    stream_state._f = function
    return stream, caller, function


@pytest.mark.asyncio
async def test_delay_drops_value_after_request_deadline() -> None:
    stream, caller, _ = _make_stream(
        _Environment(),
        timedelta(milliseconds=20),
    )
    token = request_deadline.set(
        datetime.now(timezone.utc) + timedelta(milliseconds=5)
    )
    try:
        await stream.consume(42)
    finally:
        request_deadline.reset(token)

    assert caller.values == []


@pytest.mark.asyncio
async def test_delay_drops_value_after_explicit_cancellation() -> None:
    stream, caller, _ = _make_stream(
        _Environment(),
        timedelta(milliseconds=10),
    )
    cancelled = asyncio.Event()
    token = request_cancelled.set(cancelled)
    try:
        asyncio.get_running_loop().call_later(0.002, cancelled.set)
        await stream.consume(42)
    finally:
        request_cancelled.reset(token)

    assert caller.values == []


@pytest.mark.asyncio
async def test_delay_reports_rejected_scheduling_to_delay_error() -> None:
    stream, caller, function = _make_stream(
        _RejectingEnvironment(),
        timedelta(milliseconds=10),
    )

    await stream.consume(42)

    assert caller.values == []
    assert len(function.errors) == 1
    assert str(function.errors[0]) == "request cancelled"

# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import pytest

from pyservicelib_gorundebug import transformation
from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.datasource.temporal.workflow_environment import TemporalWorkflowEnvironment
from pyservicelib_gorundebug.operators.functions import MapHandler
from pyservicelib_gorundebug.operators.map import MapStream
from pyservicelib_gorundebug.operators.substream import SubStream
from pyservicelib_gorundebug.runtime.common import (
    Collect, ServiceExecutionEnvironment, Stream, SubStreamCollectorFunc,
)
from pyservicelib_gorundebug.runtime.config import (
    MapStreamConfig, ProjectSettings, ServiceAppConfig, StreamConfig, SubStreamConfig,
)
from pyservicelib_gorundebug.runtime.context.request import (
    request_cancelled, request_deadline, request_stream_id,
)
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics, NoopMetricsEngine
from pyservicelib_gorundebug.runtime.environment.tracing import Tracing
from pyservicelib_gorundebug.runtime.serde import StreamSerde, StubSerde
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp

from .mockservice.mockservice import make_config


type Body = Callable[[Stream, int, Collect[int]], Awaitable[None]]


def config() -> ServiceAppConfig:
    service = make_config().services.income_service
    service.default_call_semantics = CallSemantics.FunctionCall
    return ServiceAppConfig(
        settings=ProjectSettings(name="substream-test"), services=[service],
        streams=[], links=[], types=[], dataConnectors=[], endpoints=[], pools=[],
        log_level="ERROR",
    )


class LocalEnvironment(ServiceApp):
    def __init__(self) -> None:
        super().__init__()
        self._config = config()
        self._serviceConfig = self._config.get_service_config_by_id(1)
        self._id = 1
        self._metrics_engine = NoopMetricsEngine()
        self._serdes["int"] = StreamSerde(StubSerde("int"))

    @property
    def tracing(self) -> Tracing | None:
        return None


def make_substream(env: ServiceExecutionEnvironment, fn: Body) -> tuple[SubStream[int, int], MapStream[int, int]]:
    cfg = env.config.config
    first = len(cfg.streams) + 1
    entry_config = StreamConfig(
        id=first, name=f"Sub{first}", type=TransformationType.SubStream,
        idService=1, idSource=first + 1, valueType="int", xPos=0, yPos=0,
    )
    body_config = StreamConfig(
        id=first + 1, name=f"Body{first}", type=TransformationType.Map,
        idService=1, idSource=first, valueType="int", xPos=0, yPos=0,
    )
    cfg.streams.extend([entry_config, body_config])
    cfg.init_runtime_config()
    entry = SubStream[int, int](SubStreamConfig(entry_config), env)
    body = MapStream[int, int](MapStreamConfig(body_config), entry, MapHandler(fn))
    entry.set_source(body)
    entry.build()
    return entry, body


async def echo(_stream: Stream, value: int, out: Collect[int]) -> None:
    await out.out(value)


@pytest.mark.asyncio
async def test_direct_call_uses_existing_links_and_public_exports() -> None:
    env = LocalEnvironment()
    entry, body = make_substream(env, echo)
    values: list[int] = []

    async def collect(value: int) -> bool:
        values.append(value)
        return True

    await entry.consume(7, SubStreamCollectorFunc(collect))
    assert values == [7]
    assert entry.type_name == "int"
    assert entry.consumers == [body]
    assert transformation.SubStream is SubStream
    assert env.config.get_substream_config(entry.name) is not None
    assert len(env._consume_statistics) == 2
    assert all(stat.count == 1 for stat in env._consume_statistics.values())


@pytest.mark.asyncio
async def test_collector_completes_call_and_drops_late_results() -> None:
    env = LocalEnvironment()

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        for delta in range(4):
            await out.out(value + delta)

    entry, _ = make_substream(env, body)
    values: list[int] = []

    async def collect(value: int) -> bool:
        values.append(value)
        return len(values) == 2

    await entry.consume(10, SubStreamCollectorFunc(collect))
    assert values == [10, 11]
    assert entry._call.get() is None


@pytest.mark.asyncio
async def test_same_substream_concurrent_calls_with_same_parent_and_stream_id() -> None:
    env = LocalEnvironment()

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        await asyncio.sleep(0)
        await out.out(value * 10)
        await asyncio.sleep(0)
        await out.out(value * 10 + 1)
        await out.out(-1)

    entry, _ = make_substream(env, body)
    token = request_stream_id.set("same-parent")

    async def invoke(value: int) -> list[int]:
        values: list[int] = []

        async def collect(result: int) -> bool:
            assert request_stream_id.get() == "same-parent"
            values.append(result)
            await asyncio.sleep(0)
            return len(values) == 2

        await entry.consume(value, SubStreamCollectorFunc(collect))
        return values

    try:
        results = await asyncio.wait_for(asyncio.gather(*(invoke(i) for i in range(100))), 5)
    finally:
        request_stream_id.reset(token)
    assert results == [[i * 10, i * 10 + 1] for i in range(100)]


@pytest.mark.asyncio
async def test_nested_body_and_collector_restore_outer_context() -> None:
    env = LocalEnvironment()
    inner, _ = make_substream(env, echo)
    outer_values: list[int] = []

    async def outer_body(_stream: Stream, value: int, out: Collect[int]) -> None:
        outer_call = outer._call.get()

        async def collect_inner(result: int) -> bool:
            assert outer._call.get() is outer_call
            assert inner._call.get() is None
            await out.out(result + 1)
            return True

        await inner.consume(value, SubStreamCollectorFunc(collect_inner))

    outer, _ = make_substream(env, outer_body)

    async def collect_outer(result: int) -> bool:
        assert outer._call.get() is None

        async def again(value: int) -> bool:
            outer_values.append(value)
            return True

        await inner.consume(result + 1, SubStreamCollectorFunc(again))
        return True

    await asyncio.wait_for(outer.consume(5, SubStreamCollectorFunc(collect_outer)), 2)
    assert outer_values == [7]


@pytest.mark.asyncio
async def test_recursive_call_to_same_substream_keeps_results_separate() -> None:
    env = LocalEnvironment()

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        if value == 0:
            await out.out(0)
            return

        async def collect(result: int) -> bool:
            await out.out(result + 1)
            return True

        await entry.consume(value - 1, SubStreamCollectorFunc(collect))

    entry, _ = make_substream(env, body)
    values: list[int] = []

    async def collect(value: int) -> bool:
        values.append(value)
        return True

    await asyncio.wait_for(entry.consume(5, SubStreamCollectorFunc(collect)), 2)
    assert values == [5]


@pytest.mark.asyncio
async def test_cancelling_one_call_does_not_cancel_sibling_or_deliver_late_values() -> None:
    env = LocalEnvironment()
    release = asyncio.Event()
    entered = asyncio.Event()
    pending: list[asyncio.Task[None]] = []

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        async def delayed() -> None:
            entered.set()
            await release.wait()
            await out.out(value)
        pending.append(asyncio.create_task(delayed()))

    entry, _ = make_substream(env, body)
    values: list[int] = []

    async def collect(value: int) -> bool:
        values.append(value)
        return True

    first = asyncio.create_task(entry.consume(1, SubStreamCollectorFunc(collect)))
    second = asyncio.create_task(entry.consume(2, SubStreamCollectorFunc(collect)))
    await entered.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    await asyncio.wait_for(second, 2)
    await asyncio.gather(*pending)
    assert values == [2]


@pytest.mark.asyncio
async def test_request_cancel_and_deadline_stop_waiting() -> None:
    env = LocalEnvironment()

    async def no_result(_stream: Stream, _value: int, _out: Collect[int]) -> None:
        return

    async def unexpected(_value: int) -> bool:
        raise AssertionError("no result expected")

    entry, _ = make_substream(env, no_result)
    cancelled = asyncio.Event()
    token = request_cancelled.set(cancelled)
    try:
        call = asyncio.create_task(entry.consume(1, SubStreamCollectorFunc(unexpected)))
        await asyncio.sleep(0)
        cancelled.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(call, 2)
    finally:
        request_cancelled.reset(token)
    deadline = request_deadline.set(datetime.now(timezone.utc) + timedelta(milliseconds=20))
    try:
        with pytest.raises(TimeoutError, match="substream deadline"):
            await entry.consume(1, SubStreamCollectorFunc(unexpected))
    finally:
        request_deadline.reset(deadline)


@pytest.mark.asyncio
async def test_callback_error_is_returned_and_binding_cleared() -> None:
    entry, _ = make_substream(LocalEnvironment(), echo)

    async def fail(_value: int) -> bool:
        raise ValueError("collector failed")

    with pytest.raises(ValueError, match="collector failed"):
        await entry.consume(1, SubStreamCollectorFunc(fail))
    assert entry._call.get() is None


@pytest.mark.asyncio
async def test_cross_loop_result_runs_collector_on_owner_loop() -> None:
    env = LocalEnvironment()
    owner = asyncio.get_running_loop()
    values: list[int] = []

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        await asyncio.to_thread(lambda: asyncio.run(out.out(value)))

    async def collect(value: int) -> bool:
        assert asyncio.get_running_loop() is owner
        values.append(value)
        return True

    entry, _ = make_substream(env, body)
    await asyncio.wait_for(entry.consume(9, SubStreamCollectorFunc(collect)), 2)
    assert values == [9]


@pytest.mark.asyncio
async def test_cancellation_drains_active_collector() -> None:
    env = LocalEnvironment()
    entered, release = asyncio.Event(), asyncio.Event()
    finished = False

    async def collect(_value: int) -> bool:
        nonlocal finished
        entered.set()
        await release.wait()
        finished = True
        return True

    entry, _ = make_substream(env, echo)
    call = asyncio.create_task(entry.consume(1, SubStreamCollectorFunc(collect)))
    await entered.wait()
    call.cancel()
    await asyncio.sleep(0)
    assert not call.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(call, 2)
    assert finished


@pytest.mark.asyncio
async def test_same_call_serializes_callbacks_but_separate_calls_overlap() -> None:
    env = LocalEnvironment()
    active = 0
    maximum = 0
    pending: list[asyncio.Task[None]] = []

    async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
        pending.extend(asyncio.create_task(out.out(value)) for _ in range(3))

    entry, _ = make_substream(env, body)

    async def invoke(value: int) -> None:
        local_active, count = 0, 0

        async def collect(result: int) -> bool:
            nonlocal active, maximum, local_active, count
            assert result == value
            local_active += 1
            assert local_active == 1
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1
            local_active -= 1
            count += 1
            return count == 2

        await entry.consume(value, SubStreamCollectorFunc(collect))
        assert count == 2

    await asyncio.wait_for(asyncio.gather(invoke(1), invoke(2)), 2)
    await asyncio.gather(*pending)
    assert maximum == 2


def test_requires_body_and_source_and_rejects_invalid_source() -> None:
    env = LocalEnvironment()
    entry, body = make_substream(env, echo)
    with pytest.raises(ValueError, match="different stream"):
        entry.set_source(entry)
    other, other_body = make_substream(env, echo)
    with pytest.raises(ValueError, match="already configured"):
        entry.set_source(other_body)
    entry.set_source(body)
    assert other.name != entry.name


@pytest.mark.asyncio
async def test_disabled_tracing_does_not_enter_span_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("disabled tracing must not build a span")

    monkeypatch.setattr("pyservicelib_gorundebug.operators.substream.start_stream_span", forbidden)
    entry, _ = make_substream(LocalEnvironment(), echo)

    async def collect(_value: int) -> bool:
        return True

    await entry.consume(1, SubStreamCollectorFunc(collect))


@pytest.mark.asyncio
async def test_workflow_wait_uses_sdk_wait_without_graph_quiescence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pyservicelib_gorundebug.datasource.temporal.workflow_environment.WorkflowMetrics", NoopMetrics)
    monkeypatch.setattr("pyservicelib_gorundebug.datasource.temporal.workflow_environment.workflow.wait", asyncio.wait)
    env = TemporalWorkflowEnvironment(config(), 1)
    completion = asyncio.Event()
    unrelated = asyncio.create_task(asyncio.Event().wait())
    env._tasks.add(unrelated)
    try:
        wait = asyncio.create_task(env.wait_substream_result(completion))
        completion.set()
        await asyncio.wait_for(wait, 1)
        assert not unrelated.done()
        failure_wait = asyncio.create_task(env.wait_substream_result(asyncio.Event()))
        await asyncio.sleep(0)
        env._record_failure(ValueError("workflow branch failed"))
        with pytest.raises(ValueError, match="workflow branch failed"):
            await asyncio.wait_for(failure_wait, 1)
    finally:
        unrelated.cancel()
        await asyncio.gather(unrelated, return_exceptions=True)


@pytest.mark.asyncio
async def test_workflow_deadline_uses_workflow_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pyservicelib_gorundebug.datasource.temporal.workflow_environment.WorkflowMetrics", NoopMetrics)
    monkeypatch.setattr("pyservicelib_gorundebug.datasource.temporal.workflow_environment.workflow.wait", asyncio.wait)
    now = datetime(2020, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr("pyservicelib_gorundebug.datasource.temporal.workflow_environment.workflow.now", lambda: now)
    env = TemporalWorkflowEnvironment(config(), 1)
    token = request_deadline.set(now - timedelta(seconds=1))
    try:
        with pytest.raises(TimeoutError, match="substream deadline"):
            await env.wait_substream_result(asyncio.Event())
        assert env.substream_now() == now
    finally:
        request_deadline.reset(token)

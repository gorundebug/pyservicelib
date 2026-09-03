import asyncio
from datetime import timedelta

import pytest

from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment.log import Field, Logger
from pyservicelib_gorundebug.runtime.serviceapp import run_shutdown_operations
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp


class RecordingLogger(Logger):
    def __init__(self) -> None:
        self.records: list[tuple[str, str, tuple[Field, ...]]] = []

    def debug(self, msg: str, *fields: Field) -> None:
        self.records.append(("debug", msg, fields))

    def info(self, msg: str, *fields: Field) -> None:
        self.records.append(("info", msg, fields))

    def warn(self, msg: str, *fields: Field) -> None:
        self.records.append(("warn", msg, fields))

    def error(self, msg: str, *fields: Field) -> None:
        self.records.append(("error", msg, fields))


def _resource(record: tuple[str, str, tuple[Field, ...]]) -> str | None:
    for field in record[2]:
        if field.key == "resource":
            return field.str_val()
    return None


@pytest.mark.asyncio
async def test_shutdown_reports_failed_resource() -> None:
    logger = RecordingLogger()

    async def fail() -> None:
        raise RuntimeError("close failed")

    await run_shutdown_operations(
        logger,
        Context(timedelta(seconds=1)),
        [("datasink:orders", fail())],
    )

    assert any(
        message == "shutdown operation failed"
        and _resource(record) == "datasink:orders"
        for record in logger.records
        for message in [record[1]]
    )


@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_resource_and_returns() -> None:
    logger = RecordingLogger()
    release = asyncio.Event()
    completed = asyncio.Event()

    async def hang() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                # A misbehaving resource may defer cancellation while it
                # releases its own state. It still cannot extend the
                # service-wide shutdown deadline.
                continue
        completed.set()

    stopped = asyncio.create_task(
        run_shutdown_operations(
            logger,
            Context(timedelta(milliseconds=5)),
            [("component:stuck", hang())],
        )
    )
    await asyncio.sleep(0.02)
    assert stopped.done()
    release.set()
    await stopped

    assert not completed.is_set()
    await asyncio.sleep(0)
    assert completed.is_set()
    assert any(
        message == "shutdown operation timed out"
        and _resource(record) == "component:stuck"
        for record in logger.records
        for message in [record[1]]
    )


def test_bounded_context_uses_shorter_deadline() -> None:
    parent = Context(timedelta(seconds=1))
    child = parent.bounded(timedelta(milliseconds=20))

    assert child.time_left is not None
    assert parent.time_left is not None
    assert child.time_left <= 0.020
    assert parent.time_left > 0.5


def test_child_context_cancellation_is_one_way() -> None:
    parent = Context(timedelta(seconds=1))
    first = parent.child()
    sibling = first

    first.cancel()

    assert sibling.cancelled
    assert sibling.is_expired
    assert not parent.cancelled

    inherited = parent.child()
    parent.cancel()
    assert inherited.cancelled


@pytest.mark.asyncio
async def test_service_opens_managed_admission_only_after_graph_is_ready() -> None:
    events: list[str] = []

    class TestApp(ServiceApp):
        @property
        def service_config(self):  # type: ignore[no-untyped-def,override]
            return type(
                "ServiceConfig",
                (),
                {
                    "status_handler": "",
                    "metrics_handler": "",
                    "startup_handler": "",
                    "readiness_handler": "",
                    "liveness_handler": "",
                },
            )()

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        async def start(self, _ctx: Context) -> None:
            events.append(f"start:{self.name}")

    class ManagedConnector(Resource):
        async def start_admission(self, _ctx: Context) -> None:
            events.append("start-admission:managed")

    app = TestApp()
    app._delay_pool = Resource("delay")  # type: ignore[assignment]
    app._storages = [Resource("storage")]  # type: ignore[assignment]
    app._task_pools = {"task": Resource("task")}  # type: ignore[assignment]
    app._priority_task_pools = {  # type: ignore[assignment]
        "priority": Resource("priority")
    }
    app._components = [Resource("component")]  # type: ignore[assignment]
    app._dataSinks = {1: Resource("sink")}  # type: ignore[assignment]
    app._managed_data_connectors = {  # type: ignore[assignment]
        1: ManagedConnector("managed")
    }
    app._dataSources = {1: Resource("source")}  # type: ignore[assignment]

    await app.start(Context(timedelta(seconds=1)))

    assert events == [
        "start:managed",
        "start:storage",
        "start:delay",
        "start:task",
        "start:priority",
        "start:component",
        "start:sink",
        "start-admission:managed",
        "start:source",
    ]


@pytest.mark.asyncio
async def test_service_stops_graph_pools_before_draining_parallel_tasks() -> None:
    events: list[str] = []
    pool_stopped = asyncio.Event()

    class TestApp(ServiceApp):
        @property
        def service_config(self):  # type: ignore[no-untyped-def,override]
            return type("ServiceConfig", (), {"shutdown_timeout": 1000})()

    class Loader:
        async def stop(self) -> None:
            return None

    class Pool:
        async def stop(self, _ctx: Context) -> None:
            events.append("pool:stop")
            pool_stopped.set()

    class Engine:
        async def shutdown(self) -> None:
            return None

    async def graph_task() -> None:
        await pool_stopped.wait()
        events.append("task:done")

    app = TestApp()
    app._loader = Loader()  # type: ignore[assignment]
    app._log = RecordingLogger()
    app._delay_pool = Pool()  # type: ignore[assignment]
    app._metrics_engine = Engine()  # type: ignore[assignment]
    app._logs_engine = Engine()  # type: ignore[assignment]
    task = asyncio.create_task(graph_task())
    app._tasks.add(task)
    task.add_done_callback(app._tasks.discard)

    await app.stop(Context(timedelta(seconds=1)))

    assert events == ["pool:stop", "task:done"]

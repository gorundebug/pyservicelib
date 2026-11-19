from typing import cast
import asyncio
from datetime import timedelta

import pytest

from pyservicelib_gorundebug.runtime.common import DataSink, DataSource, ManagedDataConnector
from pyservicelib_gorundebug.runtime.store import Storage
from pyservicelib_gorundebug.runtime.pool import TaskPool, PriorityTaskPool
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
async def test_shutdown_phase_timeout_still_joins_resource() -> None:
    logger = RecordingLogger()
    release = asyncio.Event()
    completed = asyncio.Event()

    async def hang() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                # Physical cleanup owns resources until it has finished.
                # ServiceApp bounds waiting separately from phase ownership.
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
    assert not stopped.done()
    release.set()
    await stopped
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
@pytest.mark.parametrize("graph_only", [False, True])
async def test_service_opens_managed_admission_only_after_graph_is_ready(graph_only: bool) -> None:
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

        async def start(self, ctx: Context) -> None:
            events.append(f"start:{self.name}")

        async def stop(self, ctx):
            pass

    class ManagedConnector(Resource):
        async def start_admission(self, _ctx: Context) -> None:
            events.append("start-admission:managed")

    app = TestApp()
    app._delay_pool = Resource("delay")  # type: ignore[assignment]
    app._storages = [cast(Storage, Resource("storage"))]
    app._task_pools = {"task": cast(TaskPool, Resource("task"))}
    app._priority_task_pools = {
        "priority": cast(PriorityTaskPool, Resource("priority"))
    }
    app._components = [Resource("component")]
    app._dataSinks = {1: cast(DataSink, Resource("sink"))}
    app._managed_data_connectors = {
        1: cast(ManagedDataConnector, ManagedConnector("managed"))
    }
    app._dataSources = {1: cast(DataSource, Resource("source"))}

    ctx = Context(timedelta(seconds=1))
    if graph_only:
        await app.start_graph(ctx)
        assert app._aiohttp_runner is None
    else:
        await app.start(ctx)

    expected = [
        "start:managed", "start:storage", "start:delay", "start:task",
        "start:priority", "start:component", "start:sink",
    ]
    if not graph_only:
        expected += ["start-admission:managed", "start:source"]
    assert events == expected


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


@pytest.mark.asyncio
@pytest.mark.parametrize('caller_cancel', [False, True])
async def test_service_timeout_keeps_client_until_query_finalizer_returns(caller_cancel):
    from .test_query_lifecycle import Stream
    from pyservicelib_gorundebug.dataquery import BeginRequest, make_endpoint_consumer

    entered, finalizing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    events = []
    class App(ServiceApp):
        @property
        def service_config(self): return type('Config', (), {'shutdown_timeout': 1000})()
    class Loader:
        async def stop(self): pass
    class Pool:
        async def stop(self, ctx): pass
    class Engine:
        async def shutdown(self): pass
    class Client:
        id, name = 1, 'query-client'
        async def stop_admission(self, ctx): pass
        async def stop(self, ctx): events.append('client-closed')
    class Handler:
        async def begin_request(self, stream): return BeginRequest(0)
        async def end_request(self, stream, state, error, out, errors):
            assert isinstance(error, asyncio.CancelledError)
            events.append('end-entered')
            finalizing.set()
            await release.wait()
            await out.out('completion')
            events.append('end-finished')
    async def execute(state, value, out):
        entered.set()
        try: await asyncio.Event().wait()
        finally: events.append('operation-cleanup')
    app = App()
    app._loader, app._delay_pool = Loader(), Pool()
    app._log = RecordingLogger()
    app._metrics_engine = app._logs_engine = Engine()
    app.add_managed_data_connector(Client())
    stream = Stream()
    consumer = make_endpoint_consumer(stream, Handler(), execute)
    task = asyncio.create_task(consumer.consume(None))
    app._tasks.add(task)
    task.add_done_callback(app._tasks.discard)
    try:
        async with asyncio.timeout(3):
            await entered.wait()
            if caller_cancel:
                first_stop = asyncio.create_task(app.stop(Context(timedelta(milliseconds=10))))
                await asyncio.sleep(0)
                first_stop.cancel()
                with pytest.raises(asyncio.CancelledError): await first_stop
            else:
                await app.stop(Context(timedelta(milliseconds=10)))
            await finalizing.wait()
            for _ in range(5): await asyncio.sleep(0)
            assert 'client-closed' not in events
            retry = asyncio.create_task(app.stop(Context(timedelta(seconds=1))))
            await asyncio.sleep(0)
            assert not retry.done()
            release.set()
            await retry
            await asyncio.gather(task, return_exceptions=True)
            assert events == ['operation-cleanup', 'end-entered', 'end-finished', 'client-closed']
            assert stream.values == ['completion']
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)

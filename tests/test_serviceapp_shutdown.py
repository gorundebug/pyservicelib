import asyncio
from datetime import timedelta

import pytest

from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment.log import Field, Logger
from pyservicelib_gorundebug.runtime.serviceapp import run_shutdown_operations


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

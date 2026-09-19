# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from pyservicelib_gorundebug.datasource.temporal.workflow_environment import TemporalWorkflowEnvironment
    from pyservicelib_gorundebug.runtime.common import Collect, Stream, SubStreamCollectorFunc
    from pyservicelib_gorundebug.runtime.context import Context
    from pyservicelib_gorundebug.runtime.context.request import (
        request_cancelled, request_deadline, request_stream_id,
    )

    from .test_substream import config, make_substream


@workflow.defn
class SubStreamLifecycleWorkflow:
    @workflow.run
    async def run(self, scenario: str) -> str:
        assert scenario in {"cancel", "deadline", "collector_error", "cancel_collector"}
        environment = TemporalWorkflowEnvironment(config(), 1)
        entered = asyncio.Event()
        finished = asyncio.Event()
        delivered = asyncio.Event()
        cancelled = asyncio.Event()
        closed = asyncio.Event()
        results: list[int] = []

        async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
            if scenario == "collector_error":
                await workflow.sleep(timedelta(milliseconds=2))
                await out.out(value)
                return

            async def emit_later() -> None:
                delay = 2 if scenario == "cancel_collector" else 20
                await workflow.sleep(timedelta(milliseconds=delay))
                if scenario in {"cancel", "deadline"}:
                    await closed.wait()
                await out.out(value)
                delivered.set()

            environment.create_task(emit_later)

        entry, _ = make_substream(environment, body)
        await environment.start(Context())
        parent = request_stream_id.set("lifecycle-parent")
        cancellation = request_cancelled.set(cancelled)
        deadline = request_deadline.set(
            workflow.now() + timedelta(milliseconds=5) if scenario == "deadline" else None
        )

        async def collect(value: int) -> bool:
            assert request_stream_id.get() == "lifecycle-parent"
            assert entry._call.get() is None
            entered.set()
            if scenario == "collector_error":
                raise ValueError("expected collector failure")
            if scenario == "cancel_collector":
                await workflow.sleep(timedelta(milliseconds=30))
            results.append(value)
            finished.set()
            return True

        invocation = asyncio.create_task(entry.consume(42, SubStreamCollectorFunc(collect)))
        try:
            if scenario == "cancel":
                await workflow.sleep(timedelta(milliseconds=5))
                cancelled.set()
            elif scenario == "cancel_collector":
                await entered.wait()
                cancelled.set()

            try:
                await invocation
            except asyncio.CancelledError:
                assert scenario in {"cancel", "cancel_collector"}
                if scenario == "cancel_collector":
                    assert finished.is_set(), "Consume returned before its collector finished"
            except TimeoutError:
                assert scenario == "deadline"
            except ValueError as error:
                assert scenario == "collector_error"
                assert str(error) == "expected collector failure"
            else:
                raise AssertionError(f"Consume unexpectedly succeeded: {scenario}")

            assert entry._call.get() is None
            closed.set()
            await environment.finish()
            if scenario == "cancel_collector":
                assert results == [42]
            else:
                assert results == []
            if scenario != "collector_error":
                assert delivered.is_set(), "Late branch never attempted delivery"
            if scenario in {"cancel", "deadline"}:
                assert not entered.is_set(), "Closed invocation called its collector"
            return scenario
        finally:
            closed.set()
            request_deadline.reset(deadline)
            request_cancelled.reset(cancellation)
            request_stream_id.reset(parent)
            await environment.finish()

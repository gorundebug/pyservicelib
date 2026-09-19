# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from pyservicelib_gorundebug.datasource.temporal.workflow_environment import TemporalWorkflowEnvironment
    from pyservicelib_gorundebug.runtime.common import Collect, Stream, SubStreamCollectorFunc
    from pyservicelib_gorundebug.runtime.context import Context
    from pyservicelib_gorundebug.runtime.context.request import request_stream_id

    from .test_substream import config, make_substream


@workflow.defn
class SubStreamReplayWorkflow:
    @workflow.run
    async def run(self, count: int) -> list[int]:
        environment = TemporalWorkflowEnvironment(config(), 1)

        async def body(_stream: Stream, value: int, out: Collect[int]) -> None:
            if value % 2:
                async def nested(result: int) -> bool:
                    assert request_stream_id.get() == "shared-workflow-parent"
                    await out.out(result + 10)
                    return True

                await entry.consume(value - 1, SubStreamCollectorFunc(nested))
            else:
                # Durable timers force execution across Workflow activations.
                await workflow.sleep(timedelta(milliseconds=10 + value % 3))
                await out.out(value * 10)
            # The completed invocation must not deliver a second result.
            await out.out(-1)

        entry, _ = make_substream(environment, body)
        await environment.start(Context())
        parent = request_stream_id.set("shared-workflow-parent")

        async def invoke(value: int) -> int:
            results: list[int] = []

            async def collect(result: int) -> bool:
                assert request_stream_id.get() == "shared-workflow-parent"
                assert entry._call.get() is None
                results.append(result)
                await workflow.sleep(timedelta(milliseconds=1))
                return True

            await entry.consume(value, SubStreamCollectorFunc(collect))
            assert results == [value * 10]
            return results[0]

        try:
            values = await asyncio.gather(*(invoke(value) for value in range(count)))
            assert entry._call.get() is None
            return list(values)
        finally:
            request_stream_id.reset(parent)
            await environment.finish()

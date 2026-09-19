# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.
"""Run against TEMPORAL_ADDRESS, then replay the actual server history.

Invoke with `python -m tests.run_substream_temporal` in the runtime test image.
This is an explicit integration gate, not a silently skipped unit test.
"""

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from .substream_temporal_workflow import SubStreamReplayWorkflow


async def main() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    identity = f"substream-python-{uuid4()}"
    async with Worker(
        client,
        task_queue=identity,
        workflows=[SubStreamReplayWorkflow],
    ):
        handle = await client.start_workflow(
            SubStreamReplayWorkflow.run,
            100,
            id=identity,
            task_queue=identity,
            execution_timeout=timedelta(seconds=60),
        )
        result = await handle.result()
        assert result == [value * 10 for value in range(100)]
        history = await handle.fetch_history()

    timers = sum(
        event.event_type == EventType.EVENT_TYPE_TIMER_FIRED
        for event in history.events
    )
    assert timers >= 100, f"Expected durable timer activations, got {timers}"
    await Replayer(workflows=[SubStreamReplayWorkflow]).replay_workflow(history)
    print(
        "SUBSTREAM_TEMPORAL_OK calls=100 nested=50 late_results=dropped "
        f"timers={timers} replay=ok"
    )


if __name__ == "__main__":
    asyncio.run(main())

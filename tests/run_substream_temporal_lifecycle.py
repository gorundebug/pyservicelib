# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.
"""Explicit Temporal cancellation/deadline/collector-error and replay gate."""

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from .substream_temporal_lifecycle_workflow import SubStreamLifecycleWorkflow


async def main() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    identity = f"substream-python-lifecycle-{uuid4()}"
    async with Worker(client, task_queue=identity, workflows=[SubStreamLifecycleWorkflow]):
        for scenario in ("cancel", "deadline", "collector_error", "cancel_collector"):
            handle = await client.start_workflow(
                SubStreamLifecycleWorkflow.run,
                scenario,
                id=f"{identity}-{scenario}",
                task_queue=identity,
                execution_timeout=timedelta(seconds=60),
            )
            assert await handle.result() == scenario
            history = await handle.fetch_history()
            assert any(
                event.event_type == EventType.EVENT_TYPE_TIMER_FIRED
                for event in history.events
            )
            await Replayer(workflows=[SubStreamLifecycleWorkflow]).replay_workflow(history)
            print(f"SUBSTREAM_TEMPORAL_LIFECYCLE_OK scenario={scenario} replay=ok", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

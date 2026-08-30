import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from pyservicelib_gorundebug.datasource.cron.apscheduler import (
    _CronEndpointConsumer,
    _PortableCronTrigger,
)
from pyservicelib_gorundebug.runtime.context.request import with_stream_id
from pyservicelib_gorundebug.runtime.schedule import (
    ScheduleBackend,
    new_schedule_trigger,
)


def test_portable_cron_calculates_utc_occurrence() -> None:
    trigger = _PortableCronTrigger("30 2 * * *", "UTC")

    next_fire = trigger.get_next_fire_time(
        None, datetime(2026, 3, 7, 3, tzinfo=timezone.utc)
    )

    assert next_fire is not None
    assert next_fire == datetime(
        2026, 3, 8, 2, 30, tzinfo=timezone.utc
    )


def test_portable_cron_rejects_non_utc_timezone() -> None:
    with pytest.raises(ValueError, match="timezone must be UTC"):
        _PortableCronTrigger("30 2 * * *", "Europe/Berlin")


@pytest.mark.asyncio
async def test_cron_source_waits_for_correlated_pipeline_result() -> None:
    class Endpoint:
        id = 1
        name = "cron"

        def on_pending_add(self, stream_id: str) -> None:
            assert stream_id == "request-1"

        def on_pending_remove(self, stream_id: str) -> None:
            assert stream_id == "request-1"

        def on_missing_stream_id(self) -> None:
            raise AssertionError("stream id is required")

        def on_late_result(self, stream_id: str) -> None:
            raise AssertionError(f"late result: {stream_id}")

        def on_duplicate_message_id(self, stream_id: str, message_id: str) -> None:
            raise AssertionError(f"duplicate result: {stream_id}/{message_id}")

    class Stream:
        result_consumer: Any = None

        def get_result_stream(self) -> object:
            return object()

        def set_result_consumer(self, consumer: Any) -> None:
            self.result_consumer = consumer

    invoked = asyncio.Event()

    class Function:
        async def on_trigger(self, trigger: Any, out: Any) -> None:
            del trigger, out
            invoked.set()

    stream = Stream()
    consumer: _CronEndpointConsumer[Any, str, Any] = _CronEndpointConsumer(
        Endpoint(), stream, Function()  # type: ignore[arg-type]
    )
    with_stream_id("request-1")
    trigger = new_schedule_trigger(
        1,
        "cron",
        datetime.now(timezone.utc),
        datetime.now(timezone.utc),
        ScheduleBackend.LOCAL,
    )
    activation = asyncio.create_task(consumer.on_trigger(trigger))
    await invoked.wait()
    await asyncio.sleep(0)
    assert not activation.done()

    await stream.result_consumer.consume("result")
    await asyncio.wait_for(activation, timeout=1)

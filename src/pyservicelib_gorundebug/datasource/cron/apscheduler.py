#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import asyncio
from datetime import datetime, timezone
from typing import cast

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
from apscheduler.triggers.base import BaseTrigger  # type: ignore[import-untyped]

from ...api.models.data_connector_type import DataConnectorType
from ...api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from ...api.models.schedule_overlap_policy import ScheduleOverlapPolicy
from ...runtime.common import (
    Consumer,
    RuntimeEndpointConsumer,
    ServiceExecutionEnvironment,
    TypedInputStream,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id
from ...runtime.datasource import (
    DataSourceEndpoint,
    DataSourceEndpointConsumer,
    InputDataSource,
)
from ...runtime.environment.log.log import err_field, str_field
from ...runtime.schedule import (
    ScheduleBackend,
    ScheduleTrigger,
    new_schedule_trigger,
)


class _CronDataSource(InputDataSource):
    _started: bool

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id, env)
        self._started = False

    async def start(self, ctx: Context) -> None:
        del ctx
        if self._started:
            return
        self._started = True
        try:
            for endpoint in self.endpoints:
                cast(_CronEndpoint, endpoint).start()
        except BaseException:
            await self.stop(Context())
            raise

    async def stop(self, ctx: Context) -> None:
        del ctx
        if not self._started:
            return
        self._started = False
        await asyncio.gather(
            *(cast(_CronEndpoint, endpoint).stop() for endpoint in self.endpoints)
        )


class _PortableCronTrigger(BaseTrigger):
    """Delegates the portable UTC-only cron contract to APScheduler."""

    def __init__(self, expression: str, timezone_name: str):
        if timezone_name != "UTC":
            raise ValueError("scheduled endpoint timezone must be UTC")
        self._delegate = CronTrigger.from_crontab(
            expression, timezone=timezone_name
        )

    def get_next_fire_time(
        self, previous_fire_time: datetime | None, now: datetime
    ) -> datetime | None:
        return self._delegate.get_next_fire_time(previous_fire_time, now)


class _CronEndpoint(DataSourceEndpoint):
    _consumer: "_CronEndpointConsumer | None"
    _runner: asyncio.Task[None] | None
    _active: set[asyncio.Task[None]]

    def __init__(self, datasource: _CronDataSource, endpoint_id: int):
        super().__init__(datasource, endpoint_id)
        self._consumer = None
        self._runner = None
        self._active = set()

    def start(self) -> None:
        cfg = self.config
        if not bool(getattr(cfg, "enabled", False)):
            return
        schedule = str(getattr(cfg, "schedule"))
        timezone_name = str(getattr(cfg, "timezone"))
        trigger = _PortableCronTrigger(schedule, timezone_name)
        self._runner = asyncio.create_task(
            self._run(trigger), name=f"cron:{self.datasource.name}:{self.name}"
        )
        self._runner.add_done_callback(self._task_done)

    async def stop(self) -> None:
        tasks = ([self._runner] if self._runner is not None else []) + list(
            self._active
        )
        self._runner = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active.clear()

    async def _run(self, trigger: _PortableCronTrigger) -> None:
        previous: datetime | None = None
        next_fire = trigger.get_next_fire_time(
            previous, datetime.now(timezone.utc)
        )
        while next_fire is not None:
            delay = max(
                0.0,
                (next_fire.astimezone(timezone.utc) - datetime.now(timezone.utc))
                .total_seconds(),
            )
            await asyncio.sleep(delay)
            now = datetime.now(timezone.utc)
            due: list[datetime] = []
            while next_fire is not None and next_fire.astimezone(timezone.utc) <= now:
                due.append(next_fire)
                previous = next_fire
                next_fire = trigger.get_next_fire_time(previous, now)
            if len(due) == 1:
                self._dispatch(due[0])
            elif (
                due
                and getattr(self.config, "missed_run_policy")
                == ScheduleMissedRunPolicy.FIREONCE
            ):
                self._dispatch(due[-1])

    def _dispatch(self, scheduled_at: datetime) -> None:
        if (
            self._active
            and getattr(self.config, "overlap_policy")
            == ScheduleOverlapPolicy.SKIP
        ):
            return
        task = asyncio.create_task(
            self._fire(scheduled_at), name=f"cron-fire:{self.name}"
        )
        self._active.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._active.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self.datasource.environment.log.error(
                "cron endpoint task failed",
                str_field("endpoint", self.name),
                err_field(error),
            )

    async def _fire(self, scheduled_at: datetime) -> None:
        if self._consumer is None:
            return
        fired_at = datetime.now(timezone.utc)
        with_stream_id(new_stream_id())
        started = self.on_request_start()
        error: Exception | None = None
        try:
            await self._consumer.consume(new_schedule_trigger(
                self.id,
                self.name,
                scheduled_at.astimezone(timezone.utc),
                fired_at,
                ScheduleBackend.LOCAL,
            ))
        except Exception as exc:
            error = exc
            raise
        finally:
            self.on_request_end(started, error)


class _CronEndpointConsumer[R, E](DataSourceEndpointConsumer[ScheduleTrigger, R, E]):
    @property
    def id(self) -> int:
        return self.endpoint.id


def APSchedulerEndpointConsumer[R, E](
    input_stream: TypedInputStream[ScheduleTrigger, R, E],
) -> Consumer[ScheduleTrigger]:
    env = input_stream.environment
    endpoint_cfg = env.config.get_endpoint_config_by_id(input_stream.endpoint_id)
    connector_cfg = env.config.get_data_connector_by_id(endpoint_cfg.id_data_connector)
    if connector_cfg.type != DataConnectorType.Cron:
        raise ValueError(
            f"endpoint {endpoint_cfg.name!r} does not reference a Cron data connector"
        )
    existing = env.get_datasource(connector_cfg.id)
    if existing is None:
        datasource = _CronDataSource(connector_cfg.id, env)
        env.add_datasource(datasource)
    elif isinstance(existing, _CronDataSource):
        datasource = existing
    else:
        raise ValueError(f"data source id={connector_cfg.id} is not a Cron data source")
    if datasource.get_endpoint(endpoint_cfg.id) is not None:
        raise ValueError(f"cron endpoint {endpoint_cfg.name!r} already exists")
    endpoint = _CronEndpoint(datasource, endpoint_cfg.id)
    consumer = _CronEndpointConsumer(endpoint, input_stream)
    endpoint._consumer = consumer
    endpoint.add_endpoint_consumer(consumer)
    datasource.add_endpoint(endpoint)
    env.runtime.register_endpoint_consumer(cast(RuntimeEndpointConsumer, consumer))
    return consumer

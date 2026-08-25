#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import cast

from apscheduler.events import (  # type: ignore[import-untyped]
    EVENT_JOB_SUBMITTED,
    JobSubmissionEvent,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
from apscheduler.triggers.base import BaseTrigger  # type: ignore[import-untyped]

from ...api.models.data_connector_type import DataConnectorType
from ...api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from ...api.models.schedule_overlap_policy import ScheduleOverlapPolicy
from ...runtime.common import (
    Collect,
    CollectFunc,
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
from ...runtime.environment.tracing import (
    data_source_endpoint_tracing_enabled,
    sampling_enabled,
    sampling_scope,
)
from ...runtime.schedule import (
    ScheduleBackend,
    ScheduleEndpointFunction,
    ScheduleTrigger,
    new_schedule_trigger,
)


class _CronDataSource(InputDataSource):
    _started: bool
    _scheduler: AsyncIOScheduler | None

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id, env)
        self._started = False
        self._scheduler = None

    async def start(self, ctx: Context) -> None:
        del ctx
        if self._started:
            return
        self._started = True
        scheduler = AsyncIOScheduler(timezone="UTC")
        self._scheduler = scheduler
        try:
            for endpoint in self.endpoints:
                cast(_CronEndpoint, endpoint).register(scheduler)
            scheduler.start()
        except BaseException:
            await self.stop(Context())
            raise

    async def stop(self, ctx: Context) -> None:
        del ctx
        if not self._started:
            return
        self._started = False
        scheduler = self._scheduler
        self._scheduler = None
        if scheduler is not None and scheduler.running:
            scheduler.shutdown(wait=True)
            await asyncio.sleep(0)
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
    _active: set[asyncio.Task[None]]
    _scheduled: deque[datetime]
    _job_id: str

    def __init__(self, datasource: _CronDataSource, endpoint_id: int):
        super().__init__(datasource, endpoint_id)
        self._consumer = None
        self._active = set()
        self._scheduled = deque()
        self._job_id = f"{datasource.id}:{endpoint_id}"

    def register(self, scheduler: AsyncIOScheduler) -> None:
        cfg = self.config
        if not bool(getattr(cfg, "enabled", False)):
            return
        schedule = str(getattr(cfg, "schedule"))
        timezone_name = str(getattr(cfg, "timezone"))
        trigger = _PortableCronTrigger(schedule, timezone_name)
        scheduler.add_listener(self._on_submission, EVENT_JOB_SUBMITTED)
        scheduler.add_job(
            self._scheduled_fire,
            trigger=trigger,
            id=self._job_id,
            name=f"{self.datasource.name}:{self.name}",
            coalesce=(
                getattr(cfg, "missed_run_policy")
                == ScheduleMissedRunPolicy.FIREONCE
            ),
            max_instances=(
                1
                if getattr(cfg, "overlap_policy")
                == ScheduleOverlapPolicy.SKIP
                else 1024
            ),
            misfire_grace_time=(
                None
                if getattr(cfg, "missed_run_policy")
                == ScheduleMissedRunPolicy.FIREONCE
                else 1
            ),
        )

    async def stop(self) -> None:
        tasks = list(self._active)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active.clear()
        self._scheduled.clear()

    def _on_submission(self, event: object) -> None:
        if not isinstance(event, JobSubmissionEvent) or event.job_id != self._job_id:
            return
        self._scheduled.extend(event.scheduled_run_times)

    async def _scheduled_fire(self) -> None:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError(f"cron endpoint {self.name!r} has no asyncio task")
        if not self._scheduled:
            self.datasource.environment.log.error(
                "cron endpoint fired without a scheduled occurrence",
                str_field("endpoint", self.name),
            )
            return
        scheduled_at = self._scheduled.popleft()
        self._active.add(task)
        try:
            await self._fire(scheduled_at)
        finally:
            self._active.discard(task)

    async def _fire(self, scheduled_at: datetime) -> None:
        if self._consumer is None:
            return
        with sampling_scope(
            sampling_enabled()
            or data_source_endpoint_tracing_enabled(self.environment, self.id)
        ):
            await self._fire_inner(scheduled_at)

    async def _fire_inner(self, scheduled_at: datetime) -> None:
        if self._consumer is None:
            return
        fired_at = datetime.now(timezone.utc)
        with_stream_id(new_stream_id())
        started = self.on_request_start()
        error: Exception | None = None
        try:
            await self._consumer.on_trigger(new_schedule_trigger(
                self.id,
                self.name,
                scheduled_at.astimezone(timezone.utc),
                fired_at,
                ScheduleBackend.LOCAL,
            ))
        except asyncio.CancelledError:
            error = RuntimeError("cron endpoint execution canceled")
            raise
        except Exception as exc:
            error = exc
            raise
        finally:
            self.on_request_end(started, error)


class _CronEndpointConsumer[T, R, E](DataSourceEndpointConsumer[T, R, E]):
    _function: ScheduleEndpointFunction[T]
    _out: Collect[T]

    def __init__(
        self,
        endpoint: DataSourceEndpoint,
        input_stream: TypedInputStream[T, R, E],
        function: ScheduleEndpointFunction[T],
    ) -> None:
        super().__init__(endpoint, input_stream)
        self._function = function
        self._out = CollectFunc(self.consume)

    @property
    def id(self) -> int:
        return self.endpoint.id

    async def on_trigger(self, trigger: ScheduleTrigger) -> None:
        await self._function.on_trigger(trigger, self._out)


def APSchedulerEndpointConsumer[T, R, E](
    input_stream: TypedInputStream[T, R, E],
    function: ScheduleEndpointFunction[T],
) -> Consumer[T]:
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
    consumer = _CronEndpointConsumer(endpoint, input_stream, function)
    endpoint._consumer = consumer
    endpoint.add_endpoint_consumer(consumer)
    datasource.add_endpoint(endpoint)
    env.runtime.register_endpoint_consumer(cast(RuntimeEndpointConsumer, consumer))
    return consumer

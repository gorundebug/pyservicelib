#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from datetime import datetime, timezone
from typing import cast

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

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
from ...runtime.schedule import (
    ScheduleBackend,
    ScheduleTrigger,
    new_schedule_trigger,
)


class _CronDataSource(InputDataSource):
    _scheduler: AsyncIOScheduler | None

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id, env)
        self._scheduler = None

    async def start(self, ctx: Context) -> None:
        del ctx
        if self._scheduler is not None:
            return
        scheduler = AsyncIOScheduler()
        for endpoint in self.endpoints:
            cast(_CronEndpoint, endpoint).register(scheduler)
        scheduler.start()
        self._scheduler = scheduler

    async def stop(self, ctx: Context) -> None:
        del ctx
        scheduler, self._scheduler = self._scheduler, None
        if scheduler is not None:
            scheduler.shutdown(wait=False)


class _CronEndpoint(DataSourceEndpoint):
    _consumer: "_CronEndpointConsumer | None"

    def __init__(self, datasource: _CronDataSource, endpoint_id: int):
        super().__init__(datasource, endpoint_id)
        self._consumer = None

    def register(self, scheduler: AsyncIOScheduler) -> None:
        cfg = self.config
        if not bool(getattr(cfg, "enabled", False)):
            return
        schedule = str(getattr(cfg, "schedule"))
        timezone_name = str(getattr(cfg, "timezone"))
        overlap = getattr(cfg, "overlap_policy")
        missed = getattr(cfg, "missed_run_policy")
        scheduler.add_job(
            self._fire,
            CronTrigger.from_crontab(schedule, timezone=timezone_name),
            id=str(self.id),
            name=self.name,
            replace_existing=False,
            max_instances=(1 if overlap == ScheduleOverlapPolicy.SKIP else 1000),
            coalesce=(missed == ScheduleMissedRunPolicy.FIREONCE),
            misfire_grace_time=(None if missed == ScheduleMissedRunPolicy.FIREONCE else 1),
        )

    async def _fire(self) -> None:
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
                fired_at,
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

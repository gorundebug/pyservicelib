#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Workflow-isolate implementation of the ordinary graph environment.

The environment deliberately owns no sockets, filesystem access, process-side
configuration reload or telemetry exporters.  It keeps
the same stream/caller/serde contracts as :class:`ServiceApp`, but all mutable
configuration is the immutable snapshot carried in the Workflow input and
Delay is backed by the official Temporal Workflow timer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import copy_context
from datetime import datetime, timedelta
from typing import Any, Optional, cast

from temporalio import workflow

from ...api.models.call_semantics import CallSemantics
from ...runtime.common import (
    ConsumeStatistics,
    DataSink,
    DataSource,
    ManagedDataConnector,
    RuntimeEndpointConsumer,
    RuntimeLinkInfo,
    ServiceExecutionEnvironment,
    ServiceExecutionRuntime,
    ServiceLoader,
    ServiceStream,
)
from ...runtime.config import (
    Config,
    LinkId,
    LinkConfig,
    ServiceAppConfig,
    ServiceConfig,
    TypeConfig,
)
from ...runtime.context import Context
from ...runtime.context.request import request_context_error
from ...runtime.environment import ServiceDependency
from ...runtime.environment.log import Logger, str_field
from ...runtime.environment.metrics import (
    Float64Histogram,
    Int64Counter,
    Int64Gauge,
    Metrics,
)
from ...runtime.environment.tracing import Tracing
from ...runtime.pool import PoolCancelledError, PriorityTaskPool, TaskPool
from ...runtime.serde import (
    DictSerde,
    ListSerde,
    Serializer,
    StreamSerializer,
    StubSerde,
    make_default_serde,
)
from ...runtime.store import Storage
from .workflow_telemetry import WorkflowLogger, WorkflowMetrics, WorkflowTracing


class _WorkflowPool:
    """Deterministic fixed-size task pool for one Workflow execution."""

    def __init__(
        self,
        name: str,
        executors_count: int,
        on_error: Callable[[BaseException], None],
        metrics: Metrics | None = None,
        service: str = "",
        priority: bool = False,
        now: Callable[[], datetime] = workflow.now,
    ) -> None:
        self._name = name
        self._executors_count = max(1, executors_count)
        self._queue: asyncio.PriorityQueue[
            tuple[int, int, tuple[Callable[..., Awaitable[Any]], tuple[Any, ...], dict[str, Any], Any] | None]
        ] = asyncio.PriorityQueue()
        self._on_error = on_error
        self._executors: list[asyncio.Task[None]] = []
        self._sequence = 0
        self._pending = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._started = False
        self._stopped = False
        self._now = now
        self._pool_metrics = _WorkflowPoolMetrics(
            metrics, service, name, priority
        )

    @property
    def name(self) -> str:
        return self._name

    async def start(self, ctx: Context) -> None:
        del ctx
        if self._started:
            return
        if self._stopped:
            raise RuntimeError(f"workflow task pool {self._name!r} is stopped")
        self._started = True
        self._pool_metrics.executors_target.set(self._executors_count)
        self._pool_metrics.executors_allocated.set(self._executors_count)
        self._executors = [
            asyncio.create_task(self._run()) for _ in range(self._executors_count)
        ]

    async def stop(self, ctx: Context) -> None:
        del ctx
        if self._stopped:
            return
        self._stopped = True
        await self._idle.wait()
        for _ in self._executors:
            self._queue.put_nowait((2, self._next_sequence(), None))
        if self._executors:
            await asyncio.gather(*self._executors)
        self._executors.clear()
        self._pool_metrics.executors_allocated.set(0)

    async def _enqueue(
        self,
        priority: int,
        fn: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if request_context_error() is not None:
            self._pool_metrics.task_rejected.inc()
            raise PoolCancelledError()
        if not self._started or self._stopped:
            self._pool_metrics.task_rejected.inc()
            raise RuntimeError(f"workflow task pool {self._name!r} is not running")
        self._pending += 1
        self._idle.clear()
        try:
            self._queue.put_nowait(
                (priority, self._next_sequence(), (fn, args, kwargs, copy_context()))
            )
            self._pool_metrics.queue_length.inc()
        except BaseException:
            self._task_done()
            raise

    def _next_sequence(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence

    def _task_done(self) -> None:
        self._pending -= 1
        if self._pending == 0:
            self._idle.set()

    async def _run(self) -> None:
        while True:
            _priority, _sequence, item = await self._queue.get()
            try:
                if item is None:
                    return
                self._pool_metrics.queue_length.dec()
                self._pool_metrics.executors_busy.inc()
                started = self._now()
                fn, args, kwargs, context = item
                async def invoke() -> None:
                    await fn(*args, **kwargs)

                task: asyncio.Task[None] = asyncio.create_task(
                    invoke(), context=context
                )
                try:
                    await task
                except BaseException as error:
                    self._on_error(error)
                finally:
                    self._pool_metrics.executors_busy.dec()
                    self._pool_metrics.tasks_total.inc()
                    self._pool_metrics.execution_duration.observe(
                        (self._now() - started).total_seconds()
                    )
                    self._task_done()
            finally:
                self._queue.task_done()

    async def wait_idle(self) -> None:
        await self._idle.wait()

    @property
    def has_work(self) -> bool:
        return self._pending != 0


class _WorkflowTaskPool(_WorkflowPool, TaskPool):
    async def add_task(
        self,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        await self._enqueue(1, fn, args, kwargs)


class _WorkflowPriorityTaskPool(_WorkflowPool, PriorityTaskPool):
    async def add_task(
        self,
        priority: int,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        await self._enqueue(priority, fn, args, kwargs)


class _NoopGauge(Int64Gauge):
    def set(self, v: int) -> None:
        del v

    def inc(self) -> None:
        pass

    def dec(self) -> None:
        pass

    def add(self, delta: int) -> None:
        del delta

    def sub(self, delta: int) -> None:
        del delta


class _NoopCounter(Int64Counter):
    def inc(self) -> None:
        pass

    def add(self, v: int) -> None:
        del v


class _NoopHistogram(Float64Histogram):
    def observe(self, v: float) -> None:
        del v


class _WorkflowPoolMetrics:
    queue_length: Int64Gauge
    executors_target: Int64Gauge
    executors_allocated: Int64Gauge
    executors_busy: Int64Gauge
    tasks_total: Int64Counter
    execution_duration: Float64Histogram
    task_rejected: Int64Counter

    def __init__(
        self,
        metrics: Metrics | None,
        service: str,
        name: str,
        priority: bool,
    ) -> None:
        if metrics is None:
            self.queue_length = _NoopGauge()
            self.executors_target = _NoopGauge()
            self.executors_allocated = _NoopGauge()
            self.executors_busy = _NoopGauge()
            self.tasks_total = _NoopCounter()
            self.execution_duration = _NoopHistogram()
            self.task_rejected = _NoopCounter()
            return
        kind = "priority task pool" if priority else "task pool"
        scope = metrics.scope(
            "priority_task_pool" if priority else "task_pool",
            {"service": service, "name": name},
        )
        self.queue_length = scope.gauge(
            "queue_length", f"{kind.capitalize()} wait queue length", {}
        )
        self.executors_target = scope.gauge(
            "executors_target", f"Desired number of {kind} executors", {}
        )
        self.executors_allocated = scope.gauge(
            "executors_allocated", f"Number of live {kind} executors", {}
        )
        self.executors_busy = scope.gauge(
            "executors_busy", f"Number of {kind} executors running callbacks", {}
        )
        self.tasks_total = scope.counter(
            "tasks_total", f"Total number of tasks executed by {kind}", {}
        )
        self.execution_duration = scope.histogram(
            "task_execution_duration_seconds", "Task execution duration in seconds", {}
        )
        self.task_rejected = scope.counter(
            "events_total", f"Total number of events in {kind}", {"event": "task_rejected"}
        )


class TemporalWorkflowEnvironment(ServiceExecutionEnvironment, ServiceExecutionRuntime):
    """One isolated ServiceLib graph execution inside a Temporal Workflow."""

    def __init__(self, config: ServiceAppConfig, service_id: int) -> None:
        service = config.get_service_config_by_id(service_id)
        if service is None:
            raise ValueError(f"service config {service_id} not found")
        self._config = config
        self._service_config = service
        self._streams: dict[int, ServiceStream] = {}
        self._serdes: dict[str, StreamSerializer] = {}
        self._consume_statistics: dict[LinkId, ConsumeStatistics] = {}
        self._runtime_links: list[RuntimeLinkInfo] = []
        self._endpoint_consumers: dict[int, RuntimeEndpointConsumer] = {}
        self._storages: list[Storage] = []
        self._data_sources: dict[int, DataSource] = {}
        self._data_sinks: dict[int, DataSink] = {}
        self._managed_connectors: dict[int, ManagedDataConnector] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._failure: BaseException | None = None
        self._failure_event = asyncio.Event()
        self._log = WorkflowLogger()
        self._metrics = WorkflowMetrics()
        self._tracing = WorkflowTracing()
        self._task_pools: dict[str, _WorkflowTaskPool] = {}
        self._priority_task_pools: dict[str, _WorkflowPriorityTaskPool] = {}
        self._started = False
        self._make_pools()

    @property
    def config(self) -> ServiceAppConfig:
        return self._config

    @property
    def service_config(self) -> ServiceConfig:
        return self._service_config

    @property
    def service_dependency(self) -> Optional[ServiceDependency]:
        return None

    @property
    def log(self) -> Logger:
        return self._log

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def tracing(self) -> Optional[Tracing]:
        return self._tracing

    @property
    def runtime(self) -> ServiceExecutionRuntime:
        return self

    def set_config(self, cfg: Config) -> None:
        if cfg.config is not self._config:
            raise RuntimeError("Temporal Workflow configuration is immutable")

    def reload_config(self, cfg: Config) -> None:
        self.set_config(cfg)

    async def service_init(
        self,
        name: str,
        dep: ServiceDependency,
        loader: ServiceLoader,
        cfg: Config,
    ) -> None:
        del name, dep, loader
        self.set_config(cfg)

    def register_stream(self, stream: ServiceStream) -> None:
        # Match ServiceApp exactly: virtual result/error views intentionally
        # reuse their owning configured stream id and the canonical stream is
        # registered last by the operator constructor.
        self._streams[stream.id] = stream

    def register_serde(self, type_name: str, serializer: StreamSerializer) -> None:
        self._serdes[type_name] = serializer

    def get_registered_serde(self, type_name: str) -> Optional[StreamSerializer]:
        return self._serdes.get(type_name)

    def register_consume_statistics(
        self, link_id: LinkId, statistics: ConsumeStatistics
    ) -> None:
        self._consume_statistics[link_id] = statistics

    def register_link_info(self, link_info: RuntimeLinkInfo) -> None:
        self._runtime_links.append(link_info)

    def register_endpoint_consumer(self, consumer: RuntimeEndpointConsumer) -> None:
        self._endpoint_consumers[consumer.id] = consumer

    def register_storage(self, storage: Storage) -> None:
        self._storages.append(storage)

    def get_task_pool(self, name: str) -> TaskPool:
        return self._task_pools[name]

    def get_priority_task_pool(self, name: str) -> PriorityTaskPool:
        return self._priority_task_pools[name]

    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        link = self._config.get_link(from_value, to_value)
        if link is None or link.timeout is None:
            return timedelta(milliseconds=self._service_config.default_grpc_timeout)
        return timedelta(milliseconds=link.timeout)

    def get_serde(self, type_name: str) -> Optional[Serializer]:
        del type_name
        return None

    def get_type_serde(self, type_name: str) -> Serializer:
        registered = self.get_registered_serde(type_name)
        if registered is not None:
            return cast(Serializer, registered)
        primitive = TypeConfig.is_primitive_type(type_name)
        type_config = None if primitive else self._config.get_type_by_name(type_name)
        if not primitive and type_config is None:
            raise ValueError(f"Type config {type_name!r} not found")
        if primitive:
            serde = make_default_serde(TypeConfig.get_serde_type(type_name))
        elif cast(Any, type_config).is_array:
            value_type = cast(Any, type_config).value_type
            if value_type is None:
                raise ValueError(f"Array type {type_name!r} has no value type")
            serde = ListSerde(type_name, self.get_type_serde(value_type))
        elif cast(Any, type_config).is_dict:
            key_type = cast(Any, type_config).key_type
            value_type = cast(Any, type_config).value_type
            if key_type is None or value_type is None:
                raise ValueError(f"Dict type {type_name!r} has incomplete types")
            serde = DictSerde(
                type_name,
                ListSerde(key_type, self.get_type_serde(key_type)),
                ListSerde(value_type, self.get_type_serde(value_type)),
            )
        else:
            serde = None
        return serde if serde is not None else StubSerde(type_name)

    def streams_init(self, ctx: Context) -> None:
        del ctx

    async def start(self, ctx: Context) -> None:
        if self._started:
            return
        info = workflow.info()
        self._log.info(
            "temporal workflow graph started",
            str_field("workflow_id", info.workflow_id),
            str_field("workflow_type", info.workflow_type),
        )
        for stream in self._streams.values():
            stream.build()
        for storage in self._storages:
            await storage.start(ctx)
        for task_pool in self._task_pools.values():
            await task_pool.start(ctx)
        for priority_pool in self._priority_task_pools.values():
            await priority_pool.start(ctx)
        self._started = True

    async def finish(self) -> None:
        if not self._started:
            return
        await self._wait_for_quiescence()
        context = Context()
        for task_pool in self._task_pools.values():
            await task_pool.stop(context)
        for priority_pool in self._priority_task_pools.values():
            await priority_pool.stop(context)
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        for storage in self._storages:
            await storage.stop(context)
        self._metrics.flush_observables()
        self._started = False
        if self._failure is not None:
            raise self._failure

    async def wait_for_completion(
        self, result: asyncio.Future[Any] | None
    ) -> Any:
        """Wait for the endpoint result and every asynchronous graph branch.

        A pooled or parallel failure must wake a Workflow endpoint that would
        otherwise wait forever for its result. After a result arrives we still
        wait for quiescence so a concurrent failed branch cannot be hidden by
        a faster successful result.
        """

        if result is None:
            await self._wait_for_quiescence()
            return None
        failure = asyncio.create_task(self._failure_event.wait())
        try:
            await workflow.wait(
                (result, failure),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._failure is not None:
                raise self._failure
            value = await result
            await self._wait_for_quiescence()
            return value
        finally:
            if not failure.done():
                failure.cancel()

    async def stop(self, ctx: Context) -> None:
        del ctx
        await self.finish()

    async def release(self) -> None:
        await self.finish()

    async def delay(
        self,
        duration: timedelta,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        await workflow.sleep(duration)
        await task(*args, **kwargs)

    def create_task(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async def invoke() -> Any:
            return await fn(*args, **kwargs)

        task = asyncio.create_task(invoke())
        self._tasks.add(task)

        def complete(done: asyncio.Task[Any]) -> None:
            self._tasks.discard(done)
            if done.cancelled():
                return
            try:
                error = done.exception()
            except BaseException as exception:
                self._record_failure(exception)
                return
            if error is not None:
                self._record_failure(error)

        task.add_done_callback(complete)
        return task

    def add_datasource(self, datasource: DataSource) -> None:
        self._data_sources[datasource.id] = datasource

    def get_datasource(self, id_connector: int) -> Optional[DataSource]:
        return self._data_sources.get(id_connector)

    def add_datasink(self, datasink: DataSink) -> None:
        self._data_sinks[datasink.id] = datasink

    def get_datasink(self, id_connector: int) -> Optional[DataSink]:
        return self._data_sinks.get(id_connector)

    def add_managed_data_connector(self, connector: ManagedDataConnector) -> None:
        self._managed_connectors[connector.id] = connector

    def get_managed_data_connector(
        self, id_connector: int
    ) -> Optional[ManagedDataConnector]:
        return self._managed_connectors.get(id_connector)

    def register_http_handler(
        self,
        path: str,
        handler: Callable[..., Any],
        method: str = "*",
    ) -> None:
        del path, handler, method
        raise RuntimeError("HTTP handlers are unavailable in a Temporal Workflow")

    def _make_pools(self) -> None:
        def use(semantics: CallSemantics | None, pool_name: str | None) -> None:
            if semantics not in (
                CallSemantics.TaskPool,
                CallSemantics.PriorityTaskPool,
            ):
                return
            if not pool_name:
                raise ValueError("pooled call semantics requires poolName")
            config = self._config.get_pool_by_name(pool_name)
            if config is None:
                raise ValueError(f"pool config {pool_name!r} not found")
            count = config.executors_count or 1
            if semantics == CallSemantics.TaskPool:
                self._task_pools.setdefault(
                    pool_name,
                    _WorkflowTaskPool(
                        pool_name,
                        count,
                        self._record_failure,
                        self._metrics,
                        self._service_config.name,
                        False,
                    ),
                )
            else:
                self._priority_task_pools.setdefault(
                    pool_name,
                    _WorkflowPriorityTaskPool(
                        pool_name,
                        count,
                        self._record_failure,
                        self._metrics,
                        self._service_config.name,
                        True,
                    ),
                )

        for raw_link in self._config.links:
            link = cast(LinkConfig, raw_link)
            stream_from = self._config.get_stream_config_by_id(link.var_from)
            stream_to = self._config.get_stream_config_by_id(link.to)
            if stream_from is None or stream_to is None:
                raise ValueError(
                    f"stream config for link {link.var_from}->{link.to} not found"
                )
            if stream_from.id_service == self._service_config.id:
                use(link.call_semantics, link.pool_name)
            if stream_to.id_service == self._service_config.id:
                use(link.income_call_semantics, link.income_pool_name)

    async def _wait_for_quiescence(self) -> None:
        while True:
            if self._failure is not None:
                raise self._failure
            pools: list[_WorkflowPool] = [
                *self._task_pools.values(),
                *self._priority_task_pools.values(),
            ]
            if not self._tasks and not any(pool.has_work for pool in pools):
                await asyncio.sleep(0)
                if not self._tasks and not any(pool.has_work for pool in pools):
                    return
            await asyncio.sleep(0)

    def _record_failure(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
            self._failure_event.set()

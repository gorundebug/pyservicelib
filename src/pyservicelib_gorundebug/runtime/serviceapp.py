#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import argparse
import asyncio
import os
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, Set, cast

import aiofiles  # type: ignore[import-untyped]
import yaml
from aiohttp import web
from watchfiles import Change, awatch

from ..api.models.call_semantics import CallSemantics
from .common import (
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
    Stream,
)
from .config import (
    Config,
    ConfigSettings,
    LinkConfig,
    LinkId,
    ServiceAppConfig,
    ServiceConfig,
    TypeConfig,
    apply_environment,
    replace_placeholders,
)
from .context import Context
from .environment import AdmissionLifecycle, Lifecycle, ServiceDependency
from .environment.log import Logger, LogsEngine, err_field, int_field, str_field
from .environment.metrics import MetricsEngine
from .environment.metrics.metrics import Int64Counter, Metrics
from .environment.tracing import Tracing, TracingEngine
from .logging import create_asynclog_engine
from .pool import (
    DelayPool,
    PriorityTaskPool,
    TaskPool,
    make_delay_pool,
    make_priority_task_pool,
    make_task_pool,
)
from .serde import (
    DictSerde,
    ListSerde,
    Serializer,
    StreamSerializer,
    StubSerde,
    make_default_serde,
)
from .store import JoinStorageConfig, Storage
from .telemetry import create_prometheus_metrics_engine

type ShutdownOperation = tuple[str, Coroutine[Any, Any, None]]


def consume_detached_shutdown_result(task: asyncio.Task[None]) -> None:
    if not task.cancelled():
        task.exception()


async def run_shutdown_operations(
    logger: Logger,
    ctx: Context,
    operations: list[ShutdownOperation],
) -> None:
    """Run named shutdown operations within one shared deadline."""
    tasks: dict[asyncio.Task[None], str] = {
        asyncio.create_task(operation, name=f"servicelib-shutdown:{name}"): name
        for name, operation in operations
    }
    if not tasks:
        return

    try:
        done, pending = await asyncio.wait(
            tasks,
            timeout=ctx.time_left,
        )
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
            task.add_done_callback(consume_detached_shutdown_result)
        raise

    for task in done:
        if task.cancelled():
            logger.warn(
                "shutdown operation cancelled",
                str_field("resource", tasks[task]),
            )
            continue
        error = task.exception()
        if error is not None:
            logger.warn(
                "shutdown operation failed",
                str_field("resource", tasks[task]),
                err_field(error),
            )

    for task in pending:
        logger.warn(
            "shutdown operation timed out",
            str_field("resource", tasks[task]),
        )

    if pending:
        for task in pending:
            task.cancel()
            task.add_done_callback(consume_detached_shutdown_result)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ServiceApp(ServiceExecutionEnvironment, ServiceExecutionRuntime):

    _dataSources: dict[int, DataSource]
    _dataSinks: dict[int, DataSink]
    _metrics: Metrics
    _config: ServiceAppConfig
    _serviceConfig: ServiceConfig
    _streams: dict[int, ServiceStream]
    _serdes: dict[str, StreamSerializer]
    _task_pools: dict[str, TaskPool]
    _priority_task_pools: dict[str, PriorityTaskPool]
    _loader: ServiceLoader
    _logs_engine: LogsEngine
    _metrics_engine: MetricsEngine
    _tracing_engine: Optional[TracingEngine]
    _log: Logger
    _id: int
    _delay_pool: DelayPool
    _tasks: Set[asyncio.Task[Any]]
    _storages: list[Storage]
    _consume_statistics: dict[LinkId, ConsumeStatistics]
    _runtime_links: list[RuntimeLinkInfo]
    _endpoint_consumers: dict[int, RuntimeEndpointConsumer]
    _dep: Optional[ServiceDependency]
    _components: list[Lifecycle]
    _managed_data_connectors: dict[int, ManagedDataConnector]
    _aiohttp_app: Optional[web.Application]
    _aiohttp_runner: Optional[web.AppRunner]

    def __init__(self) -> None:
        self._dataSources = {}
        self._dataSinks = {}
        self._streams = {}
        self._serdes = {}
        self._task_pools = {}
        self._priority_task_pools = {}
        self._tasks = set()
        self._storages = []
        self._consume_statistics = {}
        self._runtime_links = []
        self._endpoint_consumers = {}
        self._tracing_engine = None
        self._components = []
        self._managed_data_connectors = {}
        self._aiohttp_app = None
        self._aiohttp_runner = None

    def reload_config(self, cfg: Config) -> None:
        self._config = cfg.config
        self.on_config_reloaded()

    def on_config_reloaded(self) -> None:
        pass

    def add_component(self, component: Lifecycle) -> None:
        self._components.append(component)

    def has_custom_http_server(self) -> bool:
        return False

    def register_http_handler(
        self, path: str, handler: Callable[..., Any], method: str = "*"
    ) -> None:
        if self._aiohttp_app is None:
            raise RuntimeError("HTTP server was not initialized for application")
        self._aiohttp_app.router.add_route(method, path, handler)

    def service_context(self) -> Any:
        return self

    async def service_init(
        self,
        name: str,
        dep: Optional[ServiceDependency],
        loader: ServiceLoader,
        cfg: Config,
    ) -> None:
        self._dep = dep
        self._loader = loader
        service_config = cfg.config.get_service_config_by_name(name)
        if service_config is None:
            raise ValueError(f"Config for the service named {name} not found")
        self._id = service_config.id
        self._config = cfg.config
        logs_engine: Optional[LogsEngine] = None
        metrics_engine: Optional[MetricsEngine] = None

        tracing_engine: Optional[TracingEngine] = None

        dep = self.service_dependency
        if dep is not None:
            logs_engine = await dep.get_logs_engine(self)
            metrics_engine = await dep.get_metrics_engine(self)
            tracing_engine = await dep.get_tracing_engine(self)

        if logs_engine is None:
            self._logs_engine = await create_asynclog_engine()
        else:
            self._logs_engine = logs_engine

        if metrics_engine is None:
            self._metrics_engine = create_prometheus_metrics_engine(service_config.name)
        else:
            self._metrics_engine = metrics_engine

        self._tracing_engine = tracing_engine

        self._log = self._logs_engine.default_logger()
        self._delay_pool = make_delay_pool(self)

        for link in cast(list[LinkConfig], self._config.links):
            stream_from = self._config.get_stream_config_by_id(link.var_from)
            stream_to = self._config.get_stream_config_by_id(link.to)
            if (
                stream_from.id_service == service_config.id
                or stream_to.id_service == service_config.id
            ):
                if stream_from.id_service == service_config.id:
                    call_semantics = link.call_semantics
                else:
                    if link.income_call_semantics is None:
                        raise ValueError(
                            f"Income call semantics does not defined for link from={link.var_from} to={link.to}"
                        )
                    call_semantics = link.income_call_semantics
                if call_semantics not in [
                    CallSemantics.FunctionCall,
                    CallSemantics.TaskPool,
                    CallSemantics.PriorityTaskPool,
                    CallSemantics.ParallelCall,
                ]:
                    raise ValueError(
                        f"Invalid call semantics {call_semantics} defined for link from={link.var_from} to={link.to}"
                    )
                if call_semantics in [
                    CallSemantics.TaskPool,
                    CallSemantics.PriorityTaskPool,
                ]:
                    if stream_from.id_service == service_config.id:
                        if link.pool_name is None:
                            raise ValueError(
                                f"Pool name does not defined for link from={link.var_from} to={link.to}"
                            )
                        if (
                            call_semantics == CallSemantics.PriorityTaskPool
                            and link.priority is None
                        ):
                            raise ValueError(
                                f"Priority for link from={link.var_from} to={link.to} does not defines"
                            )
                        pool_name = link.pool_name
                    else:
                        if link.income_pool_name is None:
                            raise ValueError(
                                f"Income pool name does not defined for link from={link.var_from} to={link.to}"
                            )
                        if (
                            call_semantics == CallSemantics.PriorityTaskPool
                            and link.income_priority is None
                        ):
                            raise ValueError(
                                f"Income priority for link from={link.var_from} to={link.to} does not defines"
                            )
                        pool_name = link.income_pool_name
                    pool_cfg = self._config.get_pool_by_name(pool_name)
                    if pool_cfg is None:
                        raise ValueError(
                            f"Task pool '{pool_name}' not found for link from={link.var_from} to={link.to}"
                        )
                    if call_semantics == CallSemantics.TaskPool:
                        if pool_name not in self._task_pools:
                            self._task_pools[pool_name] = make_task_pool(
                                pool_name, self
                            )
                    elif call_semantics == CallSemantics.PriorityTaskPool:
                        if pool_name not in self._priority_task_pools:
                            self._priority_task_pools[pool_name] = (
                                make_priority_task_pool(pool_name, self)
                            )

        info_gauge = self._metrics_engine.metrics.scope(
            "service",
            {
                "service": service_config.name,
                "environment": service_config.environment.value,
            },
        ).gauge("info", "Service information (value is always 1)", {})
        info_gauge.set(1)

        if not self.has_custom_http_server():
            self._aiohttp_app = web.Application()

    async def release(self) -> None:
        pass

    @property
    def service_dependency(self) -> Optional[ServiceDependency]:
        return self._dep

    def _is_primitive_type(self, type_name: str) -> bool:
        if TypeConfig.is_primitive_type(type_name):
            return True
        tp = self.config.get_type_by_name(type_name)
        return tp is not None and tp.is_primitive

    def _get_serde_type(self, type_name: str, is_array: bool) -> str:
        if TypeConfig.is_primitive_type(type_name):
            serde_type = TypeConfig.get_serde_type(type_name)
        else:
            tp = self.config.get_type_by_name(type_name)
            if tp is None:
                raise ValueError(f"Type with name '{type_name}' not found")
            serde_type = tp.serde_type
        return f"[]{serde_type}" if is_array else serde_type

    def _make_default_serde(self, type_name: str) -> Optional[Serializer]:
        ser = self.get_serde(type_name)
        if ser is not None:
            return ser
        ser = make_default_serde(type_name)
        if ser is not None:
            return ser
        return None

    @property
    def log(self) -> Logger:
        return self._log

    def get_type_serde(self, type_name: str) -> Serializer:
        ser = self.get_serde(type_name)
        if ser is not None:
            return ser

        if self._is_primitive_type(type_name):
            ser = self._make_default_serde(self._get_serde_type(type_name, False))
        else:
            tp = self.config.get_type_by_name(type_name)
            if tp is None:
                raise ValueError(f"Type config '{type_name}' not found")

            if tp.is_array:
                if tp.value_type is None:
                    raise ValueError(f"Invalid value type for array type '{type_name}'")

                if self._is_primitive_type(tp.value_type):
                    ser = self._make_default_serde(
                        self._get_serde_type(tp.value_type, True)
                    )
                else:
                    ser = ListSerde(type_name, self.get_type_serde(tp.value_type))
            elif tp.is_dict:
                if tp.key_type is None:
                    raise ValueError(f"Invalid key type for dict type '{type_name}'")
                if tp.value_type is None:
                    raise ValueError(f"Invalid value type for dict type '{type_name}'")

                if self._is_primitive_type(tp.key_type):
                    keys_ser = self._make_default_serde(
                        self._get_serde_type(tp.key_type, True)
                    )
                else:
                    keys_ser = ListSerde(tp.key_type, self.get_type_serde(tp.key_type))

                if self._is_primitive_type(tp.value_type):
                    values_ser = self._make_default_serde(
                        self._get_serde_type(tp.value_type, True)
                    )
                else:
                    values_ser = ListSerde(
                        tp.value_type, self.get_type_serde(tp.value_type)
                    )

                if values_ser is not None and keys_ser is not None:
                    ser = DictSerde(type_name, keys_ser, values_ser)

        if ser is None:
            ser = StubSerde("")

        return ser

    def register_stream(self, stream: ServiceStream) -> None:
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
            return timedelta(seconds=self.service_config.default_grpc_timeout / 1000)
        return timedelta(seconds=link.timeout / 1000)

    def get_serde(self, type_name: str) -> Optional[Serializer]:
        return None

    def streams_init(self, ctx: Context) -> None:
        pass

    async def start(self, ctx: Context) -> None:
        from .statusweb import data_handler as _data_handler
        from .statusweb import graph_handler as _graph_handler
        from .statusweb import status_handler as _status_handler
        from .statusweb import vis_css_handler as _vis_css_handler
        from .statusweb import vis_js_handler as _vis_js_handler

        for stream in self._streams.values():
            stream.build()

        for ds in self._dataSources.values():
            await ds.start(ctx)
        for ds in self._dataSinks.values():  # type: ignore[assignment]
            await ds.start(ctx)
        for storage in self._storages:
            await storage.start(ctx)
        await self._delay_pool.start(ctx)
        for pool in self._task_pools.values():
            await pool.start(ctx)
        for pool in self._priority_task_pools.values():  # type: ignore[assignment]
            await pool.start(ctx)
        for component in self._components:
            await component.start(ctx)

        service_config = self.service_config
        if service_config.status_handler:
            status_path = "/" + service_config.status_handler.lstrip("/")
            self.register_http_handler(status_path, partial(_status_handler, self))
            self.register_http_handler(
                status_path + "/data", partial(_data_handler, self)
            )
            self.register_http_handler(
                status_path + "/graph", partial(_graph_handler, self)
            )
            self.register_http_handler(
                status_path + "/vis.min.js", partial(_vis_js_handler, self)
            )
            self.register_http_handler(
                status_path + "/vis.min.css", partial(_vis_css_handler, self)
            )

        if service_config.metrics_handler:
            metrics_path = "/" + service_config.metrics_handler.lstrip("/")
            mh = self._metrics_engine.metrics_handler

            async def _metrics_http_handler(
                request: web.Request, _h=mh
            ) -> web.Response:
                data = _h()
                return web.Response(
                    body=data,
                    headers={
                        "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
                    },
                )

            self.register_http_handler(metrics_path, _metrics_http_handler)

        health_paths = {
            "/" + configured.lstrip("/")
            for configured in (
                service_config.startup_handler,
                service_config.readiness_handler,
                service_config.liveness_handler,
            )
            if configured
        }

        async def _health_http_handler(request: web.Request) -> web.Response:
            if request.method != "GET":
                raise web.HTTPMethodNotAllowed(request.method, ["GET"])
            return web.Response(text="ok\n", content_type="text/plain")

        for health_path in health_paths:
            self.register_http_handler(health_path, _health_http_handler)

        if self._aiohttp_app is not None:
            self._aiohttp_runner = web.AppRunner(self._aiohttp_app)
            await self._aiohttp_runner.setup()
            site = web.TCPSite(
                self._aiohttp_runner, service_config.http_host, service_config.http_port
            )
            await site.start()

    async def stop(self, ctx: Context) -> None:
        ctx = ctx.bounded(timedelta(milliseconds=self.service_config.shutdown_timeout))

        # Phase 1: close network admission and let already accepted HTTP/gRPC
        # requests finish while their endpoint state, graph pools and sinks are
        # still available. Stopping those resources concurrently with the
        # listeners can discard a pending result that an active request needs.
        admission: list[ShutdownOperation] = [
            ("config_loader", self._loader.stop()),
        ]
        deferred_components: list[AdmissionLifecycle] = []
        regular_components: list[Lifecycle] = []
        for component in self._components:
            if isinstance(component, AdmissionLifecycle):
                admission.append(
                    (
                        f"component_admission:{type(component).__name__}",
                        component.stop_admission(ctx),
                    )
                )
                deferred_components.append(component)
            else:
                regular_components.append(component)
        if self._aiohttp_runner is not None:
            admission.append(("http_server", self._aiohttp_runner.cleanup()))
        await run_shutdown_operations(self._log, ctx, admission)

        # Each source now closes its own admission and drains its active root
        # invocations. Network endpoint pending-result state remains alive
        # until the HTTP/gRPC server drain above has completed.
        sources: list[ShutdownOperation] = []
        for ds in self._dataSources.values():
            sources.append((f"datasource:{ds.name}", ds.stop(ctx)))
        sources.extend(
            (f"component:{type(component).__name__}", component.stop(ctx))
            for component in regular_components
        )
        await run_shutdown_operations(self._log, ctx, sources)

        # Sources and managed pools no longer admit root work. ParallelCall
        # tasks may create nested ParallelCall tasks, so drain snapshots until
        # the service-level registry is empty before stopping sinks.
        while self._tasks:
            snapshot = tuple(self._tasks)
            done, pending = await asyncio.wait(snapshot, timeout=ctx.time_left)
            if pending:
                self._log.warn(
                    "service graph drain timed out",
                    int_field("tasks_count", len(pending)),
                )
                for task in pending:
                    task.cancel()
                    task.add_done_callback(consume_detached_shutdown_result)
                break
            await asyncio.gather(*done, return_exceptions=True)

        # Phase 2: graph admission is closed and detached work is drained;
        # pools, sinks and the remaining component/storage state can stop.
        phase2: list[ShutdownOperation] = [
            ("delay_pool", self._delay_pool.stop(ctx)),
        ]
        phase2.extend(
            (f"datasink:{ds.name}", ds.stop(ctx))
            for ds in self._dataSinks.values()
        )
        for name, pool in self._task_pools.items():
            phase2.append((f"task_pool:{name}", pool.stop(ctx)))
        for name, pool in self._priority_task_pools.items():  # type: ignore[assignment]
            phase2.append((f"priority_task_pool:{name}", pool.stop(ctx)))
        phase2.extend(
            (f"component:{type(component).__name__}", component.stop(ctx))
            for component in deferred_components
        )
        phase2.extend(
            (f"storage:{type(storage).__name__}", storage.stop(ctx))
            for storage in self._storages
        )
        await run_shutdown_operations(self._log, ctx, phase2)

        telemetry: list[ShutdownOperation] = [
            ("metrics", self._metrics_engine.shutdown()),
        ]
        if self._tracing_engine is not None:
            telemetry.append(("tracing", self._tracing_engine.shutdown()))
        await run_shutdown_operations(self._log, ctx, telemetry)
        await run_shutdown_operations(
            self._log,
            ctx,
            [("logs", self._logs_engine.shutdown())],
        )

    def add_datasource(self, datasource: DataSource) -> None:
        self._dataSources[datasource.id] = datasource

    def get_datasource(self, id_connector: int) -> Optional[DataSource]:
        return self._dataSources.get(id_connector)

    def add_datasink(self, datasink: DataSink) -> None:
        self._dataSinks[datasink.id] = datasink

    def get_datasink(self, id_connector: int) -> Optional[DataSink]:
        return self._dataSinks.get(id_connector)

    def add_managed_data_connector(self, connector: ManagedDataConnector) -> None:
        if connector.id in self._managed_data_connectors:
            raise ValueError(
                f"managed data connector id={connector.id} is already registered"
            )
        self._managed_data_connectors[connector.id] = connector
        self.add_component(connector)

    def get_managed_data_connector(
        self, id_connector: int
    ) -> Optional[ManagedDataConnector]:
        return self._managed_data_connectors.get(id_connector)

    @property
    def metrics(self) -> Metrics:
        return self._metrics_engine.metrics

    @property
    def tracing(self) -> Optional[Tracing]:
        if self._tracing_engine is None:
            return None
        return self._tracing_engine.tracing

    def set_config(self, cfg: Config) -> None:
        self._config = cfg.config

    @property
    def config(self) -> ServiceAppConfig:
        return self._config

    @property
    def service_config(self) -> ServiceConfig:
        return self._config.get_service_config_by_id(self._id)

    @property
    def runtime(self) -> "ServiceExecutionRuntime":
        return self

    async def delay(
        self, duration: timedelta, task: Callable[..., Any], *args, **kwargs
    ):
        await self._delay_pool.add_task(duration, task, *args, **kwargs)

    def create_task(self, fn: Callable[..., Any], *args, **kwargs):
        task = asyncio.create_task(fn(*args, **kwargs))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


class ServiceAppLoader[ServiceType: ServiceApp, ConfigType: ServiceAppConfig](
    ServiceLoader
):
    _watching_task: asyncio.Task[Any]
    _ready_flag: asyncio.Event
    _watching_flag: asyncio.Event
    _config_data: str
    _service: ServiceType
    _config_reload_success_counter: Int64Counter
    _config_reload_error_counter: Int64Counter

    async def _reload_from_values(self, cfg_class: Any, values_file: str) -> None:
        async with aiofiles.open(values_file, "r") as file:
            values_data = await file.read()
            values_dict: dict[str, Any] = yaml.safe_load(values_data)

        config_dict: dict[str, Any] = yaml.safe_load(self._config_data)
        result_config: dict[str, Any] = apply_environment(
            replace_placeholders(config_dict, values_dict)
        )

        cfg = cfg_class.from_dict(result_config)
        if cfg is None:
            raise ValueError("Failed to create updated config")
        self._service.runtime.reload_config(cfg)
        self._config_reload_success_counter.inc()

    async def _watch_config_changes(self, values_file: str):
        cfg_class = self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
        real_values_file = os.path.realpath(values_file)
        self._watching_flag.set()

        try:
            async for changes in awatch(Path(values_file).parent):
                if not self._ready_flag.is_set():
                    await self._ready_flag.wait()

                if self._watching_task.cancelling():
                    break

                # K8s ConfigMap atomically replaces the symlink target.
                # Detect this by checking whether the resolved real path changed.
                current_real = (
                    os.path.realpath(values_file) if os.path.exists(values_file) else ""
                )
                if current_real and current_real != real_values_file:
                    real_values_file = current_real
                    try:
                        await self._reload_from_values(cfg_class, values_file)
                    except Exception as e:
                        self._service.log.error(f"Failed to reload configuration: {e}")
                        self._config_reload_error_counter.inc()
                    continue

                for change, file_path in changes:
                    if file_path in {values_file, real_values_file} and change in {
                        Change.modified,
                        Change.added,
                    }:
                        try:
                            await self._reload_from_values(cfg_class, values_file)
                        except Exception as e:
                            self._service.log.error(
                                f"Failed to reload configuration: {e}"
                            )
                            self._config_reload_error_counter.inc()
        finally:
            try:
                self._service.log.debug("Watch config changes loop exited.")
            except AttributeError:
                pass

    def _get_path(self, arg_path: str) -> str:
        if not os.path.isabs(arg_path):
            try:
                dir_path = os.getcwd()
                file_path = os.path.join(dir_path, arg_path)
            except OSError as e:
                raise RuntimeError(f"path error: {e}")
        else:
            file_path = arg_path
        return str(Path(file_path).resolve())

    async def load(
        self, name: str, dep: ServiceDependency, config_settings: ConfigSettings
    ) -> ServiceType:
        self._service = cast(ServiceType, self.__orig_class__.__args__[0]())  # type: ignore[attr-defined]
        if not isinstance(self._service, ServiceApp):
            raise ValueError(
                "Invalid service type. Service must be inherit from ServiceApp class"
            )
        cfg_class = self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
        if not issubclass(cfg_class, ServiceAppConfig):
            raise ValueError(
                "Invalid config type. Config must be inherit from ServiceAppConfig class"
            )

        parser = argparse.ArgumentParser(description="Service configuration paths")
        parser.add_argument(
            "--config", default="./config.yaml", help="Service config path"
        )
        parser.add_argument(
            "--values", default="./values.yaml", help="Service config values path"
        )
        parser.add_argument(
            "--overrides", default=None, help="Service config overrides path"
        )
        args, _ = parser.parse_known_args()
        config_file = self._get_path(args.config)
        values_file = self._get_path(args.values)
        overrides_file = self._get_path(args.overrides) if args.overrides else None

        self._ready_flag = asyncio.Event()
        self._watching_flag = asyncio.Event()

        async with aiofiles.open(config_file, "r") as file:
            self._config_data = await file.read()
            config_dict: dict[str, Any] = yaml.safe_load(self._config_data)

        if os.path.exists(values_file):
            self._watching_task = asyncio.create_task(
                self._watch_config_changes(values_file)
            )
            if not self._watching_flag.is_set():
                await self._watching_flag.wait()

            async with aiofiles.open(values_file, "r") as file:
                values_data = await file.read()
                values_dict: dict[str, Any] = yaml.safe_load(values_data)

            config_dict = replace_placeholders(config_dict, values_dict)
        else:
            self._watching_flag.set()
            self._watching_task = asyncio.get_event_loop().create_future()  # type: ignore[assignment]
            self._watching_task.cancel()

        if overrides_file is not None:
            async with aiofiles.open(overrides_file, "r") as file:
                overrides_data = await file.read()
                overrides_dict: dict[str, Any] = yaml.safe_load(overrides_data)
            config_dict = _deep_merge(config_dict, overrides_dict)

        config_dict = apply_environment(config_dict)
        cfg = cfg_class.from_dict(config_dict)
        if cfg is None:
            raise ValueError("Failed to create config")

        await self._service.runtime.service_init(name, dep, self, cfg)
        scope = self._service.metrics.scope(
            "service", {"service": self._service.service_config.name}
        )
        self._config_reload_success_counter = scope.counter(
            "config_reloads_total",
            "Total number of config reload attempts",
            {"event": "success"},
        )
        self._config_reload_error_counter = scope.counter(
            "config_reloads_total",
            "Total number of config reload attempts",
            {"event": "error"},
        )
        self._ready_flag.set()

        return self._service

    async def stop(self):
        self._watching_task.cancel()
        try:
            await self._watching_task
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass


class JoinStreamStorageConfig(JoinStorageConfig):
    _stream: Stream

    def __init__(self, stream: Stream):
        self._stream = stream

    @property
    def ttl(self) -> timedelta:
        cfg = self._stream.config
        return (
            timedelta(milliseconds=0)
            if cfg.ttl is None
            else timedelta(milliseconds=cfg.ttl)
        )

    @property
    def renew_ttl(self) -> bool:
        cfg = self._stream.config
        return False if cfg.renew_ttl is None else cfg.renew_ttl

    @property
    def name(self) -> str:
        cfg = self._stream.config
        return cfg.name

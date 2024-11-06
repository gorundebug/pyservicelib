#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import argparse
import os
from datetime import timedelta
from pathlib import Path
from typing import cast, Optional, Any, Callable, Set

import aiofiles
import yaml
from watchfiles import awatch, Change
import asyncio

from pyservicelib.runtime.environment import ServiceDependency
from pyservicelib.runtime.config import Config, ServiceConfig, ServiceAppConfig
from pyservicelib.runtime.config import TypeConfig, ConfigSettings, replace_placeholders
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.common import DataSink, DataSource, ConsumeStatistics, ServiceLoader
from pyservicelib.runtime.common import Endpoint, ServiceStream, EndpointWriter, EndpointReader, Stream
from pyservicelib.runtime.common import ServiceExecutionRuntime, ServiceExecutionEnvironment
from pyservicelib.runtime.environment.metrics import MetricsEngine
from pyservicelib.runtime.pool import PriorityTaskPool, TaskPool, DelayPool, make_delay_pool
from pyservicelib.runtime.serde import ListSerde, DictSerde
from pyservicelib.runtime.serde import Serializer, StreamSerializer, StubSerde, make_default_serde
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.environment.metrics.metrics import Metrics
from pyservicelib.runtime.environment.log import LogsEngine, Logger
from pyservicelib.runtime.logging import LogsEngineFactory, LogsEngineType
from pyservicelib.runtime.store import JoinStorageConfig


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
    _log: Logger
    _id: int
    _delay_pool: DelayPool
    _tasks: Set[asyncio.Task[Any]]

    def __init__(self):
        self._dataSources = {}
        self._dataSinks = {}
        self._streams = {}
        self._serdes = {}
        self._task_pools = {}
        self._priority_task_pools = {}
        self._tasks = set()

    def reload_config(self, cfg: Config) -> None:
        pass

    async def service_init(self, name: str, dep: Optional[ServiceDependency], loader: ServiceLoader, cfg: Config) -> None:
        self._loader = loader
        service_config = cfg.config.get_service_config_by_name(name)
        if service_config is None:
            raise ValueError(f"Config for the service named {name} not found")
        self._id = service_config.id
        self._config = cfg.config
        logs_engine: Optional[LogsEngine] = None
        metrics_engine: Optional[MetricsEngine] = None
        if dep is not None:
            logs_engine = await dep.get_logs_engine(self)
            metrics_engine = await dep.get_metrics_engine(self)

        if logs_engine is None:
            self._logs_engine = await LogsEngineFactory.create_logs_engine(LogsEngineType.ASYNCLOG)
        else:
            self._logs_engine = logs_engine

        if metrics_engine is None:
            pass
        else:
            self._metrics_engine = metrics_engine

        self._log = await self._logs_engine.default_logger()
        self._delay_pool = make_delay_pool(self)

    async def release(self) -> None:
        await self._logs_engine.release()

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
        return f'[]{serde_type}' if is_array else serde_type

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
                    ser = self._make_default_serde(self._get_serde_type(tp.value_type, True))
                else:
                    ser = ListSerde(type_name, self.get_type_serde(tp.value_type))
            elif tp.is_dict:
                if tp.key_type is None:
                    raise ValueError(f"Invalid key type for dict type '{type_name}'")
                if tp.value_type is None:
                    raise ValueError(f"Invalid value type for dict type '{type_name}'")

                if self._is_primitive_type(tp.key_type):
                    keys_ser = self._make_default_serde(self._get_serde_type(tp.key_type, True))
                else:
                    keys_ser = ListSerde(tp.key_type, self.get_type_serde(tp.key_type))

                if self._is_primitive_type(tp.value_type):
                    values_ser = self._make_default_serde(self._get_serde_type(tp.value_type, True))
                else:
                    values_ser = ListSerde(tp.value_type, self.get_type_serde(tp.value_type))

                if values_ser is not None and keys_ser is not None:
                    ser = DictSerde(type_name, keys_ser, values_ser)

        if ser is None:
            ser = StubSerde("")

        return ser

    def register_stream(self, stream: ServiceStream) -> None:
        self._streams[stream.id] = stream

    def register_serde(self, type_name: str, serializer: StreamSerializer) -> None:
        self._serdes[type_name] = serializer

    def get_registered_serde(self, type_name: str) -> StreamSerializer:
        return self._serdes[type_name]

    def register_consume_statistics(self, statistics: ConsumeStatistics) -> None:
        pass

    def register_storage(self, storage: Storage) -> None:
        pass

    def get_task_pool(self, name: str) -> TaskPool:
        return self._task_pools[name]

    def get_priority_task_pool(self, name: str) -> PriorityTaskPool:
        return self._priority_task_pools[name]

    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        link = self._config.get_link(from_value, to_value)
        if link is None or link.timeout is None:
            return timedelta(seconds=self._serviceConfig.default_grpc_timeout / 1000)
        return timedelta(seconds=link.timeout / 1000)

    def get_serde(self, type_name: str) -> Optional[Serializer]:
        return None

    def streams_init(self, ctx: Context) -> None:
        pass

    async def start(self, ctx: Context) -> None:
        await self._delay_pool.start(ctx)

    async def stop(self, ctx: Context) -> None:
        await self._loader.stop()

    def add_datasource(self, datasource: DataSource) -> None:
        self._dataSources[datasource.id] = datasource

    def get_datasource(self, id_datasource: int) -> DataSource:
        return self._dataSources[id_datasource]

    def add_datasink(self, datasink: DataSink) -> None:
        self._dataSinks[datasink.id] = datasink

    def get_datasink(self, id_datasink: int) -> DataSink:
        return self._dataSinks[id_datasink]

    def get_endpoint_reader(self, endpoint: Endpoint, stream: Stream, value_type: type) -> Optional[EndpointReader]:
        return None

    def get_endpoint_writer(self, endpoint: Endpoint, stream: Stream, value_type: type) -> Optional[EndpointWriter]:
        return None

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def set_config(self, cfg: Config) -> None:
        pass

    @property
    def config(self) -> ServiceAppConfig:
        return self._config

    @property
    def service_config(self) -> ServiceConfig:
        return self._config.get_service_config_by_id(self._id)

    @property
    def runtime(self) -> "ServiceExecutionRuntime":
        return self

    async def delay(self, duration: timedelta, task: Callable[..., Any], *args, **kwargs):
        await self._delay_pool.add_task(duration, task, *args, **kwargs)

    def create_task(self, fn: Callable[..., Any], *args, **kwargs):
        task = asyncio.create_task(fn(*args, **kwargs))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


class ServiceAppLoader[ServiceType: ServiceApp, ConfigType: ServiceAppConfig](ServiceLoader):
    _watching_task: asyncio.Task[Any]
    _ready_flag: asyncio.Event
    _watching_flag: asyncio.Event
    _config_data: str
    _service: ServiceType

    async def _watch_config_changes(self, values_file: str):
        cfg_class = self.__orig_class__.__args__[1]  #type: ignore[attr-defined]
        self._watching_flag.set()

        try:
            async for changes in awatch(Path(values_file).parent):
                if not self._ready_flag.is_set():
                    await self._ready_flag.wait()

                if self._watching_task.cancelling():
                    break

                for change, file_path in changes:
                    if file_path == values_file and change in {Change.modified, Change.added}:
                        try:
                            async with aiofiles.open(values_file, 'r') as file:
                                values_data = await file.read()
                                values_dict: dict[str, Any] = yaml.safe_load(values_data)

                            config_dict: dict[str, Any] = yaml.safe_load(self._config_data)
                            result_config: dict[str, Any] = replace_placeholders(config_dict, values_dict)

                            cfg = cfg_class.from_dict(result_config)
                            if cfg is None:
                                raise ValueError("Failed to create updated config")
                            self._service.runtime.reload_config(cfg)
                        except Exception as e:
                            self._service.log.error(f"Failed to reload configuration: {e}")
        finally:
            self._service.log.debug("Watch config changes loop exited.")

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

    async def init(self, name: str, dep: ServiceDependency, config_settings: ConfigSettings) -> ServiceType:
        self._service = cast(ServiceType, self.__orig_class__.__args__[0]())  #type: ignore[attr-defined]
        if not isinstance(self._service , ServiceApp):
            raise ValueError("Invalid service type. Service must be inherit from ServiceApp class")
        cfg_class = self.__orig_class__.__args__[1]  #type: ignore[attr-defined]
        if not issubclass(cfg_class , ServiceAppConfig):
            raise ValueError("Invalid config type. Config must be inherit from ServiceAppConfig class")

        parser = argparse.ArgumentParser(description="Service configuration paths")
        parser.add_argument("--values", default="./values.yaml", help="Service config values path")
        parser.add_argument("--config", default="./config.yaml", help="Service config path")
        args, _ = parser.parse_known_args()
        config_file = self._get_path(args.config)
        values_file = self._get_path(args.values)

        self._ready_flag = asyncio.Event()
        self._watching_flag = asyncio.Event()

        async with aiofiles.open(config_file, 'r') as file:
            self._config_data = await file.read()
            config_dict: dict[str, Any] = yaml.safe_load(self._config_data)

        self._watching_task = asyncio.create_task(self._watch_config_changes(values_file))
        if not self._watching_flag.is_set():
            await self._watching_flag.wait()

        async with aiofiles.open(values_file, 'r') as file:
            values_data = await file.read()
            values_dict: dict[str, Any] = yaml.safe_load(values_data)

        result_config: dict[str, Any] = replace_placeholders(config_dict, values_dict)

        cfg = cfg_class.from_dict(result_config)
        if cfg is None:
            raise ValueError("Failed to create config")

        await self._service.runtime.service_init(name, dep, self, cfg)
        self._ready_flag.set()

        return self._service

    async def stop(self):
        self._watching_task.cancel()
        try:
            await self._watching_task
        except asyncio.CancelledError:
            self._service.log.debug("Watching task cancelled")


class JoinStreamStorageConfig(JoinStorageConfig):
    _stream: Stream

    def __init__(self, stream: Stream):
        self._stream = stream

    @property
    def ttl(self) -> timedelta:
        cfg = self._stream.config
        return timedelta(milliseconds=0) if cfg.ttl is None else timedelta(milliseconds=cfg.ttl)

    @property
    def renew_ttl(self) -> bool:
        cfg = self._stream.config
        return False if cfg.renew_ttl is None else cfg.renew_ttl

    @property
    def name(self) -> str:
        cfg = self._stream.config
        return cfg.name


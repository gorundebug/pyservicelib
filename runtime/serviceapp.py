#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from typing import Dict, cast, Optional, Any
import argparse
import yaml
import os
import aiofiles

from pyservicelib.runtime.environment import ServiceExecutionRuntime, ServiceExecutionEnvironment
from pyservicelib.runtime.telemetry.metrics import Metrics
from pyservicelib.runtime.environment import  Endpoint, Stream, EndpointWriter, EndpointReader
from pyservicelib.runtime.environment import DataSink, DataSource, ConsumeStatistics
from pyservicelib.runtime.serde import Serializer, StreamSerializer, StubSerde, make_default_serde
from pyservicelib.runtime.serde import ListSerde, DictSerde
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool  import PriorityTaskPool, TaskPool
from pyservicelib.runtime.config import Config, ServiceConfig, ServiceAppConfig
from pyservicelib.runtime.config import TypeConfig, ConfigSettings, replace_placeholders


class ServiceApp(ServiceExecutionEnvironment, ServiceExecutionRuntime):
    _dataSources: Dict[int, DataSource]
    _dataSinks: Dict[int, DataSink]
    _metrics: Metrics
    _config: ServiceAppConfig
    _serviceConfig: ServiceConfig
    _streams: Dict[int, Stream]
    _serdes: Dict[str, StreamSerializer]
    _task_pools: Dict[str, TaskPool]
    _priority_task_pools: Dict[str, PriorityTaskPool]

    def __init__(self):
        self._dataSources = {}
        self._dataSinks = {}
        self._streams = {}
        self._serdes = {}
        self._task_pools = {}
        self._priority_task_pools = {}

    def reload_config(self, cfg: Config) -> None:
        pass

    def service_init(self, name: str, cfg: Config) -> None:
        pass

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
                    ser = ListSerde(self.get_type_serde(tp.value_type))
            elif tp.is_dict:
                if tp.key_type is None:
                    raise ValueError(f"Invalid key type for dict type '{type_name}'")
                if tp.value_type is None:
                    raise ValueError(f"Invalid value type for dict type '{type_name}'")

                if self._is_primitive_type(tp.key_type):
                    keys_ser = self._make_default_serde(self._get_serde_type(tp.value_type, True))
                else:
                    keys_ser = ListSerde(self.get_type_serde(tp.key_type))

                if self._is_primitive_type(tp.value_type):
                    values_ser = self._make_default_serde(self._get_serde_type(tp.value_type, True))
                else:
                    values_ser = ListSerde(self.get_type_serde(tp.value_type))

                if values_ser is not None and keys_ser is not None:
                    ser = DictSerde(keys_ser, values_ser)

        if ser is None:
            ser = StubSerde()

        return ser

    def register_stream(self, stream: Stream) -> None:
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

    def start(self, ctx: Context) -> None:
        pass

    def stop(self, ctx: Context) -> None:
        pass

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
        return self._serviceConfig

    @property
    def runtime(self) -> "ServiceExecutionRuntime":
        return self


def get_path(arg_path: str) -> str:
    if not os.path.isabs(arg_path):
        try:
            dir_path = os.getcwd()
            file_path = os.path.join(dir_path, arg_path)
        except OSError as e:
            raise RuntimeError(f"path error: {e}")
    else:
        file_path = arg_path
    return file_path

class ServiceLoader[ServiceType: ServiceApp, ConfigType: ServiceAppConfig]:

    async def init(self, name: str, config_settings: ConfigSettings) -> ServiceType:
        service = self.__orig_class__.__args__[0]()  #pyright: ignore
        if not isinstance(service , ServiceApp):
            raise ValueError("Invalid service type. Service must be inherit from ServiceApp class")
        cfg_class = self.__orig_class__.__args__[1]  #pyright: ignore
        if not issubclass(cfg_class , ServiceAppConfig):
            raise ValueError("Invalid config type. Config must be inherit from ServiceAppConfig class")

        parser = argparse.ArgumentParser(description="Service configuration paths")
        parser.add_argument("--values", default="./values.yaml", help="Service config values path")
        parser.add_argument("--config", default="./config.yaml", help="Service config path")
        args, _ = parser.parse_known_args()
        config_file = get_path(args.config)
        values_file = get_path(args.values)

        async with aiofiles.open(config_file, 'r') as file:
            content = await file.read()
            config_data: Dict[str, Any] = yaml.safe_load(content)

        async with aiofiles.open(values_file, 'r') as file:
            content = await file.read()
            values_data: Dict[str, Any] = yaml.safe_load(content)

        result_config: Dict[str, Any] = replace_placeholders(config_data, values_data)

        cfg = cfg_class.from_dict(result_config)
        if cfg is None:
            raise ValueError("Failed to create config")

        service.runtime.service_init(name, cfg)

        return cast(ServiceType, service)

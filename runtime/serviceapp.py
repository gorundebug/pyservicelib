#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from datetime import timedelta
from typing import Dict

from typing_extensions import Optional

from environment import ServiceExecutionRuntime
from pyservicelib.runtime import ServiceExecutionEnvironment
from pyservicelib.runtime.telemetry.metrics import Metrics
from pyservicelib.runtime.environment import  Endpoint, Stream, EndpointWriter, EndpointReader
from pyservicelib.runtime.environment import DataSink, DataSource, ConsumeStatistics
from pyservicelib.runtime.serde import Serializer, StreamSerializer
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool  import PriorityTaskPool, TaskPool
from pyservicelib.runtime.config import Config, ServiceConfig, ServiceAppConfig


class ServiceApp(ServiceExecutionEnvironment, ServiceExecutionRuntime):
    _dataSources: Dict[int, DataSource]
    _dataSinks: Dict[int, DataSink]
    _metrics: Metrics
    _config: ServiceAppConfig
    _serviceConfig: ServiceConfig
    _streams: Dict[int, Stream]
    _serdes: Dict[type, StreamSerializer]
    _task_pools: Dict[str, TaskPool]
    _priority_task_pools: Dict[str, PriorityTaskPool]

    def __init__(self):
        self._dataSources = {}
        self._dataSinks = {}
        self._streams = {}
        self._serdes = {}
        self._task_pools = {}
        self._priority_task_pools = {}

    def reload_config(self, config: Config) -> None:
        pass

    def service_init(self, name: str, config: Config) -> None:
        pass

    def get_type_serde(self, value_type: type) -> Serializer:
        ser = self.get_serde(value_type)
        if ser is not None:
            return ser

        raise ValueError(f"Serde for type '{value_type.__name__}' not found")

    def register_stream(self, stream: Stream) -> None:
        self._streams[stream.id] = stream

    def register_serde(self, value_type: type, serializer: StreamSerializer) -> None:
        self._serdes[value_type] = serializer

    def get_registered_serde(self, value_type: type) -> StreamSerializer:
        return self._serdes[value_type]

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

    def get_serde(self, value_type: type) -> Optional[Serializer]:
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

    def set_config(self, config: Config) -> None:
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
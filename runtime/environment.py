#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List, Optional

from pyservicelib.runtime.config import StreamConfig, LinkId, Config, ServiceEnvironmentConfig
from pyservicelib.runtime.serde import Serializer, StreamSerializer
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool import TaskPool, PriorityTaskPool
from pyservicelib.runtime.telemetry.metrics import Metrics
from pyservicelib.runtime.context import Context
from collections.abc import Hashable
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.config import EndpointConfig, DataConnectorConfig

class DataConnector(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def id(self) -> int:
        pass

class Endpoint(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def id(self) -> int:
        pass

    @property
    @abstractmethod
    def data_connector(self) -> DataConnector:
        pass

class DataSource(DataConnector):

    @abstractmethod
    def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def stop(self, ctx: Context) -> None:
        pass

    @property
    @abstractmethod
    def data_connector(self) -> DataConnectorConfig:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "StreamExecutionRuntime":
        pass

    @abstractmethod
    def add_endpoint(self, endpoint: "InputEndpoint") -> None:
        pass

    @abstractmethod
    def get_endpoint(self, id_endpoint: int) -> "InputEndpoint":
        pass

    @property
    @abstractmethod
    def endpoints(self) -> List["InputEndpoint"]:
        pass

class InputEndpointConsumer(ABC):

    @property
    @abstractmethod
    def endpoint(self) -> "InputEndpoint":
        pass


class InputEndpoint(Endpoint):

    @property
    @abstractmethod
    def config(self) -> EndpointConfig:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "StreamExecutionRuntime":
        pass

    @property
    @abstractmethod
    def datasource(self) -> DataSource:
        pass

    @abstractmethod
    def add_endpoint_consumer(self, consumer: InputEndpointConsumer) -> None:
        pass

    @property
    @abstractmethod
    def endpoint_consumers(self) -> List[InputEndpointConsumer]:
        pass


class DataSink(DataConnector):

    @abstractmethod
    def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def stop(self, ctx: Context) -> None:
        pass

    @property
    @abstractmethod
    def data_connector(self) -> DataConnectorConfig:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "StreamExecutionRuntime":
        pass

    @abstractmethod
    def add_endpoint(self, endpoint: "SinkEndpoint") -> None:
        pass

    @abstractmethod
    def get_endpoint(self, id_endpoint: int) -> "SinkEndpoint":
        pass

    @property
    @abstractmethod
    def endpoints(self) -> List["SinkEndpoint"]:
        pass

class OutputEndpointConsumer(ABC):

    @property
    @abstractmethod
    def endpoint(self) -> "SinkEndpoint":
        pass


class SinkEndpoint(Endpoint):

    @property
    @abstractmethod
    def config(self) -> EndpointConfig:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "StreamExecutionRuntime":
        pass

    @property
    @abstractmethod
    def datasource(self) -> DataSink:
        pass

    @abstractmethod
    def add_endpoint_consumer(self, consumer: OutputEndpointConsumer) -> None:
        pass

    @property
    @abstractmethod
    def endpoint_consumers(self) -> List[OutputEndpointConsumer]:
        pass


class EndpointReader(ABC):
    pass


class EndpointWriter(ABC):
    pass


class Stream(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def transformation_name(self) -> str:
        pass

    @property
    @abstractmethod
    def type_name(self) -> str:
        pass

    @property
    @abstractmethod
    def id(self) -> int:
        pass

    @property
    @abstractmethod
    def config(self) -> StreamConfig:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "StreamExecutionRuntime":
        pass


class ServiceStream(Stream):

    @property
    @abstractmethod
    def consumers(self) -> List[Stream]:
        pass


class Consumer[T](ABC):
    @abstractmethod
    def consume(self, item: T) -> None:
        pass


class TypedStreamConsumer[T](Stream, Consumer[T], ABC):
    pass


class TypedStream[T](Stream):

    @property
    @abstractmethod
    def consumer(self):
        pass

    @consumer.setter
    @abstractmethod
    def consumer(self, value):
        pass

    @property
    @abstractmethod
    def serde(self):
        pass


class TypedConsumedStream[T](TypedStream[T], Consumer[T], ABC):
    pass


class TypedTransformConsumedStream[T, R](TypedStream[R], Consumer[T], ABC):
    pass


class TypedJoinConsumedStream[K: Hashable, T1, T2, R](TypedTransformConsumedStream[KeyValue[K, T1], R]):

    @abstractmethod
    def consume_right(self, kv: KeyValue[K, T2]) -> None:
        pass


class TypedMultiJoinConsumedStream[K: Hashable, T, R](TypedTransformConsumedStream[KeyValue[K, T], R]):

    @abstractmethod
    def consume_right(self, kv: KeyValue[K, T]) -> None:
        pass

class TypedLinkStream[T](TypedStream[T], Consumer[T]):

    @abstractmethod
    def set_source(self, stream: TypedConsumedStream[T]) -> None:
        pass


class TypedSplitStream[T](TypedConsumedStream[T]):

    @abstractmethod
    def add_stream(self) -> TypedConsumedStream[T]:
        pass

class BinaryConsumer(ABC):

    @abstractmethod
    def consume_binary(self, data: bytes) -> None:
        pass


class BinaryKVConsumer(ABC):

    @abstractmethod
    def consume_binary(self, key_data: bytes, value_data: bytes) -> None:
        pass


class TypedBinaryConsumedStream[T](TypedConsumedStream[T], BinaryConsumer, ABC):
   pass


class TypedBinaryKVConsumedStream[T](TypedConsumedStream[T], BinaryKVConsumer, ABC):
    pass


class TypedBinarySplitStream[T](TypedBinaryConsumedStream[T]):

    @abstractmethod
    def add_stream(self) -> TypedConsumedStream[T]:
        pass


class TypedBinaryKVSplitStream[T](TypedBinaryKVConsumedStream[T]):

    @abstractmethod
    def add_stream(self) -> TypedConsumedStream[T]:
        pass


class StreamExecutionEnvironment(ServiceEnvironmentConfig):
    @abstractmethod
    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        pass

    @abstractmethod
    def get_serde(self, value_type: type) -> Optional[Serializer]:
        pass

    @abstractmethod
    def streams_init(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def stop(self, ctx: Context) -> None:
        pass

    @abstractmethod
    def add_datasource(self, datasource: DataSource) -> None:
        pass

    @abstractmethod
    def get_datasource(self, id_datasource: int) -> DataSource:
        pass

    @abstractmethod
    def add_datasink(self, datasink: DataSink) -> None:
        pass

    @abstractmethod
    def get_datasink(self, id_datasink: int) -> DataSink:
        pass

    @abstractmethod
    def get_endpoint_reader(self, endpoint: Endpoint, stream: Stream, value_type: type) -> Optional[EndpointReader]:
        pass

    @abstractmethod
    def get_endpoint_writer(self, endpoint: Endpoint, stream: Stream, value_type: type) -> Optional[EndpointWriter]:
        pass

    @property
    @abstractmethod
    def metrics(self) -> Metrics:
        pass

    @abstractmethod
    def set_config(self, config: Config) -> None:
        pass

class Caller[T](ABC):

    @abstractmethod
    def consume(self, value: T) ->None:
        pass

class ConsumeStatistics(ABC):

    @property
    @abstractmethod
    def count(self) -> int:
        pass

    @property
    @abstractmethod
    def link_id(self) -> LinkId:
        pass

class StreamExecutionRuntime(StreamExecutionEnvironment):

    @abstractmethod
    def _reload_config(self, config: Config) -> None:
        pass

    @abstractmethod
    def _service_init(self, name: str,  config: Config) -> None:
        pass

    @abstractmethod
    def _get_serde(self, value_type: type) -> Serializer:
        pass

    @abstractmethod
    def _register_stream(self, stream: ServiceStream) -> None:
        pass

    @abstractmethod
    def _register_serde(self, value_type: type, serializer: StreamSerializer) -> None:
        pass

    @abstractmethod
    def _get_registered_serde(self, value_type: type) -> StreamSerializer:
        pass

    @abstractmethod
    def _register_consume_statistics(self, statistics: ConsumeStatistics) -> None:
        pass

    @abstractmethod
    def _register_storage(self, storage: Storage) -> None:
        pass

    @abstractmethod
    def _get_task_pool(self, name: str) -> TaskPool:
        pass

    @abstractmethod
    def _get_priority_task_pool(self, name: str) -> PriorityTaskPool:
        pass
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List
from pyservicelib.runtime.config import StreamConfig, Config
from pyservicelib.runtime import StreamExecutionRuntime
from pyservicelib.runtime.serde import Serializer
from pyservicelib.runtime import DataSource
from pyservicelib.runtime import DataSink
from pyservicelib.runtime.telemetry.metrics import Metrics
from pyservicelib.runtime.context import Context

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
    def runtime(self) -> StreamExecutionRuntime:
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


class StreamExecutionEnvironment(ABC):

    @abstractmethod
    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        pass

    @abstractmethod
    def get_serde(self, value_type: type) -> Serializer:
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
    def get_endpoint_reader(self, endpoint: Endpoint, stream: Stream, value_type: type) -> EndpointReader:
        pass

    @abstractmethod
    def get_endpoint_writer(self, endpoint: Endpoint, stream: Stream, value_type: type) -> EndpointWriter:
        pass

    @property
    @abstractmethod
    def metrics(self) -> Metrics:
        pass

    @abstractmethod
    def set_config(self, config: Config) -> None:
        pass

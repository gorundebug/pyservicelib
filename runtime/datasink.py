#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from pyservicelib.runtime import Endpoint, StreamExecutionRuntime, DataConnector
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.config import EndpointConfig, DataConnectorConfig
from typing import List

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
    def runtime(self) -> StreamExecutionRuntime:
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
    def runtime(self) -> StreamExecutionRuntime:
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

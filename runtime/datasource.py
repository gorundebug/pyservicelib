#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import ValuesView, Iterable

from pyservicelib.runtime import ServiceExecutionEnvironment, Consumer
from pyservicelib.runtime.common import DataSource, InputEndpoint, ServiceExecutionRuntime, DataConnector, \
    InputEndpointConsumer, OutputEndpointConsumer
from pyservicelib.runtime.config import DataConnectorConfig, EndpointConfig
from pyservicelib.runtime.context import Context


class InputDataSource(DataSource):
    _id: int
    _environment: ServiceExecutionEnvironment
    _endpoints: dict[int, InputEndpoint]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        self._id = connector_id
        self._environment = env
        self._endpoints = {}

    def start(self, ctx: Context) -> None:
        pass

    def stop(self, ctx: Context) -> None:
        pass

    @property
    def data_connector(self) -> DataConnectorConfig:
        return self._environment.config.get_data_connector_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._environment

    def add_endpoint(self, endpoint: InputEndpoint) -> None:
        self._endpoints[endpoint.id] = endpoint

    def get_endpoint(self, id_endpoint: int) -> InputEndpoint:
        return self._endpoints[id_endpoint]

    @property
    def endpoints(self) ->  Iterable[InputEndpoint]:
        return self._endpoints.values()

    @property
    def name(self) -> str:
        return self.data_connector.name

    @property
    def id(self) -> int:
        return self._id


class DataSourceEndpoint(InputEndpoint):
    _id: int
    _data_source: DataSource
    _endpoint_consumers: list[InputEndpointConsumer]

    def __init__(self, data_source: DataSource, id_endpoint: int):
        self._id = id_endpoint
        self._data_source = data_source
        self._endpoint_consumers = []

    @property
    def config(self) -> EndpointConfig:
        return self.environment.config.get_endpoint_config_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._data_source.environment

    @property
    def datasource(self) -> DataSource:
        return self._data_source

    def add_endpoint_consumer(self, consumer: InputEndpointConsumer) -> None:
        self._endpoint_consumers.append(consumer)

    @property
    def endpoint_consumers(self) -> Iterable[InputEndpointConsumer]:
        return self._endpoint_consumers

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def id(self) -> int:
        return self._id

    @property
    def data_connector(self) -> DataConnector:
        return self.datasource


# class DataSourceEndpointConsumer[T](Consumer[T], OutputEndpointConsumer):

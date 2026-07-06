#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from abc import ABC
from typing import Optional, Iterable, cast, Any

from .common import ServiceExecutionEnvironment, Consumer, TypedInputStream
from .common import DataSource, InputEndpoint, DataConnector, TypedEndpointReader
from .common import InputEndpointConsumer, StreamContext, CollectFunc, ErrorStream
from .config import DataConnectorConfig, EndpointConfig


class InputDataSource(DataSource, ABC):
    _id: int
    _environment: ServiceExecutionEnvironment
    _endpoints: dict[int, InputEndpoint]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        self._id = connector_id
        self._environment = env
        self._endpoints = {}

    @property
    def data_connector(self) -> DataConnectorConfig:
        return self._environment.config.get_data_connector_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._environment

    def add_endpoint(self, endpoint: InputEndpoint) -> None:
        self._endpoints[endpoint.id] = endpoint

    def get_endpoint(self, id_endpoint: int) -> Optional[InputEndpoint]:
        return self._endpoints.get(id_endpoint)

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
    _datasource: DataSource
    _endpoint_consumers: list[InputEndpointConsumer]

    def __init__(self, datasource: DataSource, id_endpoint: int):
        self._id = id_endpoint
        self._datasource = datasource
        self._endpoint_consumers = []

    @property
    def config(self) -> EndpointConfig:
        return self.environment.config.get_endpoint_config_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._datasource.environment

    @property
    def datasource(self) -> DataSource:
        return self._datasource

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
        return self._datasource


class DataSourceEndpointConsumer[T, R=Any, E=Any](Consumer[T], InputEndpointConsumer):
    _input_stream: TypedInputStream[T, R, E]
    _endpoint: InputEndpoint
    _reader: Optional[TypedEndpointReader[T]]

    def __init__(self, endpoint: InputEndpoint, input_stream: TypedInputStream[T, R, E]):
        self._endpoint = endpoint
        self._input_stream = input_stream
        value_type = input_stream.config.value_type
        if value_type is None:
            raise ValueError(f"Value type can not be none for input stream '{input_stream.name}'")
        reader = input_stream.environment.get_endpoint_reader(self.endpoint,
                                                              self._input_stream, value_type)
        if reader is not None:
            if not isinstance(reader, TypedEndpointReader) or reader.type_name != value_type:
                raise ValueError(f"Invalid endpoint reader type in DataSourceEndpointConsumer")
            self._reader = cast(TypedEndpointReader[T], reader)
        else:
            self._reader = None

    async def consume(self, value: T) -> None:
        await self._input_stream.consume(value)

    @property
    def stream(self) -> TypedInputStream[T, R, E]:
        return self._input_stream

    @property
    def endpoint(self) -> InputEndpoint:
        return self._endpoint

    @property
    def reader(self) -> Optional[TypedEndpointReader[T]]:
        return self._reader


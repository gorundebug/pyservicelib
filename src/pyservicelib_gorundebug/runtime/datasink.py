#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from abc import ABC
from typing import Optional, Iterable, cast

from .common import ServiceExecutionEnvironment, Consumer, TypedEndpointWriter
from .common import DataConnector, DataSink
from .common import SinkEndpoint, OutputEndpointConsumer, TypedSinkStream
from .config import DataConnectorConfig, EndpointConfig


class OutputDataSink(DataSink, ABC):
    _id: int
    _environment: ServiceExecutionEnvironment
    _endpoints: dict[int, SinkEndpoint]

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

    def add_endpoint(self, endpoint: SinkEndpoint) -> None:
        self._endpoints[endpoint.id] = endpoint

    def get_endpoint(self, id_endpoint: int) -> Optional[SinkEndpoint]:
        return self._endpoints.get(id_endpoint)

    @property
    def endpoints(self) ->  Iterable[SinkEndpoint]:
        return self._endpoints.values()

    @property
    def name(self) -> str:
        return self.data_connector.name

    @property
    def id(self) -> int:
        return self._id


class DataSinkEndpoint(SinkEndpoint):
    _id: int
    _data_sink: DataSink
    _endpoint_consumers: list[OutputEndpointConsumer]

    def __init__(self, data_sink: DataSink, id_endpoint: int):
        self._id = id_endpoint
        self._data_sink = data_sink
        self._endpoint_consumers = []

    @property
    def config(self) -> EndpointConfig:
        return self.environment.config.get_endpoint_config_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._data_sink.environment

    @property
    def datasink(self) -> DataSink:
        return self._data_sink

    def add_endpoint_consumer(self, consumer: OutputEndpointConsumer) -> None:
        self._endpoint_consumers.append(consumer)

    @property
    def endpoint_consumers(self) -> Iterable[OutputEndpointConsumer]:
        return self._endpoint_consumers

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def id(self) -> int:
        return self._id

    @property
    def data_connector(self) -> DataConnector:
        return self._data_sink


class DataSinkEndpointConsumer[T](Consumer[T], OutputEndpointConsumer):
    _sink_stream: TypedSinkStream[T]
    _endpoint: SinkEndpoint
    _writer: Optional[TypedEndpointWriter[T]]

    def __init__(self, endpoint: SinkEndpoint, sink_stream: TypedSinkStream[T]):
        self._endpoint = endpoint
        self._sink_stream = sink_stream
        value_type = sink_stream.config.value_type
        if value_type is None:
            raise ValueError(f"Value type can not be none for sink stream '{sink_stream.name}'")
        writer = sink_stream.environment.get_endpoint_writer(self.endpoint,
                                                              self._sink_stream, value_type)
        if writer is not None:
            if not isinstance(writer, TypedEndpointWriter) or writer.type_name != value_type:
                raise ValueError(f"Invalid endpoint writer type in DataSinkEndpointConsumer")
            self._writer = cast(TypedEndpointWriter[T], writer)

    async def consume(self, value: T) -> None:
        await self._sink_stream.consume(value)

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

    @property
    def writer(self) -> Optional[TypedEndpointWriter[T]]:
        return self._writer


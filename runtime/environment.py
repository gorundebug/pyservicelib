#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List, Optional
from typing import cast

from pyservicelib.runtime.config import StreamConfig, LinkId, Config, ServiceEnvironmentConfig
from pyservicelib.runtime.serde import Serializer, StreamSerializer, TypedStreamSerde, SerdeHelpers
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool import TaskPool, PriorityTaskPool
from pyservicelib.runtime.telemetry.metrics import Metrics
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.config import EndpointConfig, DataConnectorConfig

class Consumer[T](ABC):
    @abstractmethod
    def consume(self, value: T) -> None:
        pass

class Caller[T](Consumer[T], ABC):
    pass

class DirectCaller[T](Caller[T]):

    def consume(self, value: T):
        pass

class RuntimeHelpers[T]:
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, env: "ServiceExecutionEnvironment"):
        self._environment = env

    def get_registered_serde(self, stream_name: str) -> TypedStreamSerde[T]:
        return cast(TypedStreamSerde[T],
                    self._environment.runtime.get_registered_serde(SerdeHelpers[T]().get_type(),
                                                                   stream_name))

    def make_serde(self, stream_name: str) -> TypedStreamSerde[T]:
        return self.get_registered_serde(stream_name)

    def make_caller(self, source: "TypedStream[T]") -> Caller[T]:
        return DirectCaller[T]()


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
    def runtime(self) -> "ServiceExecutionRuntime":
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
    def runtime(self) -> "ServiceExecutionRuntime":
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
    def runtime(self) -> "ServiceExecutionRuntime":
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
    def runtime(self) -> "ServiceExecutionRuntime":
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
    _config: StreamConfig
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, name: str, env: "ServiceExecutionEnvironment"):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"Stream configuration with name '{name}' not found")
        self._config = cfg
        self._environment = env
        env.runtime.register_stream(self)

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def transformation_name(self) -> str:
        return self.transformation_name

    @property
    @abstractmethod
    def type_name(self) -> str:
       pass

    @property
    def id(self) -> int:
        return self._config.id

    @property
    def config(self) -> StreamConfig:
        return self._config

    @property
    def environment(self) -> "ServiceExecutionEnvironment":
        return self._environment

    @property
    @abstractmethod
    def consumers(self) -> List["Stream"]:
        pass


class StreamConsumer[T](Consumer[T], ABC):
    _stream:  Stream

    def __init__(self, stream: Stream):
        self._stream = stream

    @property
    def stream(self) -> Stream:
        return self._stream


class TypedStream[T](Stream):
    _consumer: Optional[StreamConsumer[T]]
    _serde:  TypedStreamSerde[T]

    def __init__(self, name: str, serde: TypedStreamSerde[T], env: "ServiceExecutionEnvironment"):
        super().__init__(name, env)
        self._consumer = None
        self._serde = serde

    @property
    def consumer(self) -> Optional[StreamConsumer[T]]:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[T]):
        self._consumer = value

    @property
    def serde(self) -> TypedStreamSerde[T]:
        return self._serde

    @property
    def consumers(self) -> List[Stream]:
        if self._consumer is None:
            return []
        return [self._consumer.stream]

    @property
    def type_name(self) -> str:
        genetic_type = SerdeHelpers[T]().get_type() #pyright: ignore
        return genetic_type.__name__


class TypedConsumedStream[T](TypedStream[T], StreamConsumer[T], ABC):
    _caller: Optional[Caller[T]]

    def __init__(self, name: str, serde: TypedStreamSerde[T], env: "ServiceExecutionEnvironment"):
        ABC.__init__(self)
        TypedStream[T].__init__(self, name, serde, env)
        StreamConsumer[T].__init__(self, self)

        self._caller = None

    @property
    def consumer(self) -> Optional[StreamConsumer[T]]:
        return super().consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[T]):
        self._caller = RuntimeHelpers[T](self.environment).make_caller(self)
        super().consumer = value


class TypedTransformConsumedStream[T, R](TypedStream[R], StreamConsumer[T], ABC):
    _caller: Optional[Caller[R]]

    def __init__(self, name: str, serde: TypedStreamSerde[R], env: "ServiceExecutionEnvironment"):
        ABC.__init__(self)
        TypedStream[R].__init__(self, name, serde, env)
        StreamConsumer[T].__init__(self, self)
        self._caller = None

    @property
    def consumer(self) -> Optional[StreamConsumer[R]]:
        return super().consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[R]):
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)
        super().consumer = value


class ServiceExecutionEnvironment(ServiceEnvironmentConfig):
    @abstractmethod
    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        pass

    @abstractmethod
    def get_serde(self, value_type: type, stream_name: str) -> Optional[Serializer]:
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

    @property
    @abstractmethod
    def runtime(self) -> "ServiceExecutionRuntime":
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

class ServiceExecutionRuntime(ABC):
    @abstractmethod
    def reload_config(self, config: Config) -> None:
        pass

    @abstractmethod
    def service_init(self, name: str,  config: Config) -> None:
        pass

    @abstractmethod
    def get_type_serde(self, value_type: type, stream_name: str) -> Serializer:
        pass

    @abstractmethod
    def register_stream(self, stream: Stream) -> None:
        pass

    @abstractmethod
    def register_serde(self, value_type: type, stream_name: str, serializer: StreamSerializer) -> None:
        pass

    @abstractmethod
    def get_registered_serde(self, value_type: type, stream_name: str) -> StreamSerializer:
        pass

    @abstractmethod
    def register_consume_statistics(self, statistics: ConsumeStatistics) -> None:
        pass

    @abstractmethod
    def register_storage(self, storage: Storage) -> None:
        pass

    @abstractmethod
    def get_task_pool(self, name: str) -> TaskPool:
        pass

    @abstractmethod
    def get_priority_task_pool(self, name: str) -> PriorityTaskPool:
        pass


class StreamFunction[T]:
    _context: TypedStream[T]

    def __init__(self, context: TypedStream[T]):
        self._context = context

    def before_call(self):
        pass

    def after_call(self):
        pass
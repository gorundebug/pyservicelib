#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional, Callable, Any, get_origin
from typing import cast

from pyservicelib.runtime.environment import ServiceEnvironment, ServiceDependency
from pyservicelib.runtime.config import StreamConfig, LinkId, Config
from pyservicelib.runtime.serde import Serializer, StreamSerializer, TypedStreamSerde
from pyservicelib.runtime.serde import TypedStreamKeyValueSerde, StubSerde, StreamSerde, Serde
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool import TaskPool, PriorityTaskPool
from pyservicelib.runtime.environment.metrics import Metrics
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.config import EndpointConfig, DataConnectorConfig

class Consumer[T](ABC):
    @abstractmethod
    async def consume(self, value: T) -> None:
        pass


class Caller[T](Consumer[T], ABC):
    pass


class DirectCaller[T](Caller[T]):
    async def consume(self, value: T):
        pass


class Collect[T](ABC):
    @abstractmethod
    async def out(self, value: T) -> None:
        pass


class RuntimeHelpers[T]:
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, env: "ServiceExecutionEnvironment"):
        self._environment = env

    def get_registered_serde(self, type_name: str) -> TypedStreamSerde[T]:
        return cast(TypedStreamSerde[T],
                    self._environment.runtime.get_registered_serde(type_name))

    def make_serde(self, type_name: str) -> TypedStreamSerde[T]:
        ser = self.get_registered_serde(type_name)
        if ser is not None:
            return ser
        ser = cast(Serde[T], self._environment.runtime.get_type_serde(type_name))
        if ser is not None:
            pass
        if ser is None:
            ser = StubSerde()
        stream_ser = StreamSerde(ser)
        self._environment.runtime.register_serde(type_name, stream_ser)
        return stream_ser

    def make_key_value_serde(self, key_type_name: str, value_type_name: str) -> TypedStreamKeyValueSerde[T]:
        return cast(TypedStreamKeyValueSerde[T],
                    self.get_registered_serde(f"KeyValue[{key_type_name},{value_type_name}]"))

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
    def endpoints(self) -> list["InputEndpoint"]:
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
    def endpoint_consumers(self) -> list[InputEndpointConsumer]:
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
    def endpoints(self) -> list["SinkEndpoint"]:
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
    def endpoint_consumers(self) -> list[OutputEndpointConsumer]:
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
    def environment(self) -> "ServiceExecutionEnvironment":
        pass

    @property
    @abstractmethod
    def consumers(self) -> list["Stream"]:
        pass

class ServiceStream(Stream):
    _id: int
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment"):
        self._id = stream_id
        self._environment = env
        env.runtime.register_stream(self)

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def transformation_name(self) -> str:
        return self.config.transformation_name

    @property
    @abstractmethod
    def type_name(self) -> str:
       pass

    @property
    def id(self) -> int:
        return self._id

    @property
    def config(self) -> StreamConfig:
        return self.environment.config.get_stream_config_by_id(self._id)

    @property
    def environment(self) -> "ServiceExecutionEnvironment":
        return self._environment

    @property
    def consumers(self) -> list["Stream"]:
        return []

    def build(self):
        pass


class StreamConsumer[T](Consumer[T]):

    @property
    @abstractmethod
    def stream(self) -> Stream:
        pass


class TypedStream[T](ServiceStream):
    _serde:  TypedStreamSerde[T]

    def __init__(self, stream_id: int, serde: TypedStreamSerde[T], env: "ServiceExecutionEnvironment"):
        super().__init__(stream_id=stream_id, env=env)
        self._serde = serde

    @property
    @abstractmethod
    def consumer(self) -> Optional[StreamConsumer[T]]:
        pass

    @consumer.setter
    @abstractmethod
    def consumer(self, value: StreamConsumer[T]):
        pass

    @property
    def serde(self) -> TypedStreamSerde[T]:
        return self._serde

    @property
    def consumers(self) -> list[Stream]:
        return []

    @property
    def type_name(self) -> str:
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is not None:
            return orig_type.__name__
        return genetic_type.__name__


class TypedConsumedStream[T](TypedStream[T], StreamConsumer[T], ABC):
    _caller: Optional[Caller[T]]
    _consumer: Optional[StreamConsumer[T]]

    def __init__(self, stream_id: int, serde: TypedStreamSerde[T], env: "ServiceExecutionEnvironment"):
        super().__init__(stream_id=stream_id, serde=serde, env=env)
        self._caller = None

    @property
    def consumer(self) -> Optional[StreamConsumer[T]]:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[T]):
        self._caller = RuntimeHelpers[T](self.environment).make_caller(self)
        self._consumer = value

    @property
    def stream(self) -> Stream:
        return self

    @property
    def consumers(self) -> list[Stream]:
        if self._consumer is None:
            return []
        return [self._consumer.stream]


class TypedTransformConsumedStream[T, R](TypedStream[R], StreamConsumer[T], ABC):
    _caller: Optional[Caller[R]]
    _consumer: Optional[StreamConsumer[R]]

    def __init__(self, stream_id: int, serde: TypedStreamSerde[R], env: "ServiceExecutionEnvironment"):
        super().__init__(stream_id=stream_id, serde=serde, env=env)
        self._caller = None

    @property
    def consumer(self) -> Optional[StreamConsumer[R]]:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[R]):
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)
        self._consumer = value

    @property
    def stream(self) -> Stream:
        return self

    @property
    def consumers(self) -> list[Stream]:
        if self._consumer is None:
            return []
        return [self._consumer.stream]


class ServiceExecutionEnvironment(ServiceEnvironment):
    @abstractmethod
    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        pass

    @abstractmethod
    def get_serde(self, type_name: str) -> Optional[Serializer]:
        pass

    @abstractmethod
    def streams_init(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def release(self) -> None:
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
    def set_config(self, cfg: Config) -> None:
        pass

    @property
    @abstractmethod
    def runtime(self) -> "ServiceExecutionRuntime":
        pass

    @abstractmethod
    async def delay(self, duration: timedelta, task: Callable[..., Any], *args, **kwargs):
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


class ServiceLoader(ABC):

    @abstractmethod
    async def stop(self):
        pass


class ServiceExecutionRuntime(ABC):
    @abstractmethod
    def reload_config(self, cfg: Config) -> None:
        pass

    @abstractmethod
    async def service_init(self, name: str, dep: ServiceDependency, loader: ServiceLoader, cfg: Config) -> None:
        pass

    @abstractmethod
    def get_type_serde(self, type_name: str) -> Optional[Serializer]:
        pass

    @abstractmethod
    def register_stream(self, stream: ServiceStream) -> None:
        pass

    @abstractmethod
    def register_serde(self, type_name: str, serializer: StreamSerializer) -> None:
        pass

    @abstractmethod
    def get_registered_serde(self, type_name: str) -> StreamSerializer:
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

    @abstractmethod
    def create_task(self, fn: Callable[..., Any], *args, **kwargs):
        pass


class StreamFunction[T]:
    _context: TypedStream[T]

    def __init__(self, context: TypedStream[T]):
        self._context = context

    def before_call(self):
        pass

    def after_call(self):
        pass


class Collector[T](Collect[T]):
    _caller: Caller[T]

    def __init__(self, caller: Caller[T]):
        self._caller = caller

    async def out(self, value: T):
        if self._caller is not None:
            await self._caller.consume(value)


class ParallelsCollector[T](Collect[T]):
    _caller: Caller[T]
    _env: ServiceExecutionEnvironment

    def __init__(self, caller: Caller[T], env: ServiceExecutionEnvironment):
        self._caller = caller
        self._env = env

    async def consume(self, value: T):
        await self._caller.consume(value)

    async def out(self, value: T):
        self._env.runtime.create_task(self.consume, value)
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional, Callable, Any, get_origin, Hashable, Protocol
from typing import cast, ValuesView, Iterable

from pyservicelib.api.models.call_semantics import CallSemantics
from pyservicelib.runtime.environment import ServiceEnvironment, ServiceDependency
from pyservicelib.runtime.config import StreamConfig, Config, LinkId
from pyservicelib.runtime.serde import Serializer, StreamSerializer, TypedStreamSerde, StreamKeyValueSerde
from pyservicelib.runtime.serde import TypedStreamKeyValueSerde, StreamSerde, Serde
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool import TaskPool, PriorityTaskPool
from pyservicelib.runtime.environment.metrics import Metrics
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.config import EndpointConfig, DataConnectorConfig
from pyservicelib.runtime.serde import BytesBuffer
from pyservicelib.runtime.utils.atomicint import AtomicInteger


class Consume[T](Protocol):

    async def consume(self, value: T) -> None:
        ...

class Consumer[T](ABC):
    @abstractmethod
    async def consume(self, value: T) -> None:
        pass


class ConsumerBinary[T](ABC):
    @abstractmethod
    async def consume_binary(self, data: BytesBuffer) -> None:
        pass


class ConsumerBinaryKV[T](ABC):
    @abstractmethod
    async def consume_binary(self, key_data: BytesBuffer, value_data: BytesBuffer) -> None:
        pass


class BinaryConsume[T](Protocol):

    async def consume(self, data: BytesBuffer) -> None:
        ...


class BinaryKVConsume[T](Protocol):

    async def consume(self, key_data: BytesBuffer, value_data: BytesBuffer) -> None:
        ...


class ConsumeStatistics(ABC):

    @property
    @abstractmethod
    def count(self) -> int:
        pass


class CallerStatistics(ConsumeStatistics):

    _count: AtomicInteger

    def __init__(self):
        self._count = AtomicInteger()

    @property
    def count(self) -> int:
        return self._count.get()

    def inc(self, amount: int = 1):
        self._count.inc(amount)


class Caller[T](Consumer[T], ABC):

    _statistics: CallerStatistics
    _source: "TypedStream[T]"
    _consumer: "StreamConsumer[T]"

    def __init__(self, source: "TypedStream[T]", statistics: CallerStatistics):
        self._source = source
        if source.consumer is None:
            raise ValueError(f"The source stream named '{source.name}' does not have consumer in Caller constructor")
        self._consumer = source.consumer
        self._statistics = statistics


class DirectCaller[T](Caller[T]):

    def __init__(self, source: "TypedStream[T]", statistics: CallerStatistics):
        super().__init__(source=source, statistics=statistics)

    async def consume(self, value: T):
        await self._consumer.consume(value)
        self._statistics.inc()


class TaskPoolCaller[T](Caller[T]):
    _task_pool: TaskPool

    def __init__(self, task_pool: TaskPool, source: "TypedStream[T]", statistics: CallerStatistics):
        super().__init__(source=source, statistics=statistics)
        self._task_pool = task_pool

    async def consume(self, value: T):
        await self._task_pool.add_task(self.consume, value)
        self._statistics.inc()


class PriorityTaskPoolCaller[T](Caller[T]):
    _priority_task_pool: PriorityTaskPool
    _priority: int

    def __init__(self, priority_task_pool: PriorityTaskPool,
                 priority: int,
                 source: "TypedStream[T]",
                 statistics: CallerStatistics):
        super().__init__(source=source, statistics=statistics)
        self._priority_task_pool = priority_task_pool
        self._priority = priority

    async def consume(self, value: T):
        await self._priority_task_pool.add_task(self._priority, self.consume, value)
        self._statistics.inc()


class Collect[T](ABC):

    @abstractmethod
    async def out(self, value: T) -> None:
        pass


class RuntimeHelpers[T]:
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, env: "ServiceExecutionEnvironment"):
        self._environment = env

    def get_registered_serde(self, type_name: str) -> Optional[TypedStreamSerde[T]]:
        ser = self._environment.runtime.get_registered_serde(type_name)
        if ser is not None:
            return cast(TypedStreamSerde[T], ser)
        return None

    def make_stream_serde(self, type_name: str) -> TypedStreamSerde[T]:
        ser = self.get_registered_serde(type_name)
        if ser is not None:
            return ser
        ser_typed = cast(Serde[T], self._environment.runtime.get_type_serde(type_name))
        stream_ser = StreamSerde(ser_typed)
        self._environment.runtime.register_serde(type_name, stream_ser)
        return stream_ser

    def make_caller(self, source: "TypedStream[T]") -> Caller[T]:
        env = source.environment
        runtime = env.runtime
        cfg = env.config
        service_config = env.service_config
        consumer = source.consumer
        if consumer is None:
            raise ValueError(f"The source stream named '{source.name}' does not have consumer in make_caller")

        link = cfg.get_link(source.id, consumer.stream.id)
        if link is None:
            raise ValueError(f"No link found between streams from={source.id} to={consumer.stream.id}")
        stream_cfg = source.config
        if stream_cfg.id_service == service_config.id:
            call_semantics = link.call_semantics
        else:
            if link.income_call_semantics is None:
                raise ValueError(f"Invalid income call semantics for link from={source.id} to={consumer.stream.id}")
            call_semantics = link.income_call_semantics

        statistics = CallerStatistics()

        if call_semantics == CallSemantics.FunctionCall:
            return DirectCaller[T](source=source, statistics=statistics)

        elif call_semantics == CallSemantics.TaskPool:

            pool_name = link.pool_name if stream_cfg.id_service == service_config.id else link.income_pool_name
            if pool_name is None:
                raise ValueError(f"Invalid {" " if stream_cfg.id_service == service_config.id else " income "}\
pool name for link between streams from={source.id} to={consumer.stream.id}")

            return TaskPoolCaller[T](task_pool=runtime.get_task_pool(pool_name), source=source, statistics=statistics)

        elif call_semantics == CallSemantics.PriorityTaskPool:

            pool_name = link.pool_name if stream_cfg.id_service == service_config.id else link.income_pool_name
            if pool_name is None:
                raise ValueError(f"Invalid {" " if stream_cfg.id_service == service_config.id else " income "} priority\
task pool name for link between streams from={source.id} to={consumer.stream.id}")

            priority = link.priority if stream_cfg.id_service == service_config.id else link.income_priority
            if priority is None:
                raise ValueError(f"Invalid {" " if stream_cfg.id_service == service_config.id else " income "} priority\
for link between streams from={source.id} to={consumer.stream.id}")

            return PriorityTaskPoolCaller[T](priority_task_pool=runtime.get_priority_task_pool(pool_name),
                                             priority=priority,
                                             source=source,
                                             statistics=statistics)

        raise ValueError(f"Invalid call semantics: {call_semantics}")


class RuntimeKeyValueHelpers[K: Hashable, V](RuntimeHelpers[KeyValue[K, V]]):

    def __init__(self, env: "ServiceExecutionEnvironment"):
        super().__init__(env)

    def make_key_value_stream_serde(self, key_type_name: str,
                                    value_type_name: str) -> TypedStreamKeyValueSerde[KeyValue[K, V]]:
        ser = cast(TypedStreamKeyValueSerde[KeyValue[K, V]],
                   self.get_registered_serde(f"KeyValue[{key_type_name},{value_type_name}]"))
        if ser is not None:
            return ser
        key_ser = cast(Serde[K], self._environment.runtime.get_type_serde(key_type_name))
        value_ser = cast(Serde[V], self._environment.runtime.get_type_serde(value_type_name))
        stream_ser = StreamKeyValueSerde[K, V](key_ser, value_ser)
        return stream_ser


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
    def environment(self) -> "ServiceExecutionEnvironment":
        pass

    @abstractmethod
    def add_endpoint(self, endpoint: "InputEndpoint") -> None:
        pass

    @abstractmethod
    def get_endpoint(self, id_endpoint: int) -> "InputEndpoint":
        pass

    @property
    @abstractmethod
    def endpoints(self) -> Iterable["InputEndpoint"]:
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
    def environment(self) -> "ServiceExecutionEnvironment":
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
    def endpoint_consumers(self) -> Iterable[InputEndpointConsumer]:
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


class TypedEndpointReader[T](EndpointReader):

    @abstractmethod
    def read(self, b: BytesBuffer) -> T:
        pass


class TypedEndpointWriter[T](EndpointWriter):

    @abstractmethod
    def write(self, b: BytesBuffer) -> bytearray:
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

class ServiceStream(Stream, ABC):
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
    _serde:  Optional[TypedStreamSerde[T]]

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: Optional[TypedStreamSerde[T]] = None):
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
        if self._serde is None:
            raise ValueError("serde must be initialized for TypedStream")
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


class TypedLinkStream[T](TypedStream[T], StreamConsumer[T]):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment"):
        super().__init__(stream_id=stream_id, env=env)

    @abstractmethod
    def set_source(self, stream: TypedStream[T]):
        pass

    @property
    def stream(self) -> Stream:
        return self


class TypedSinkStream[T](TypedStream[T], StreamConsumer[T]):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @property
    @abstractmethod
    def endpoint_id(self) -> int:
        pass

    @property
    def stream(self) -> Stream:
        return self


class TypedConsumedStream[T](TypedStream[T], StreamConsumer[T], ABC):
    _caller: Optional[Caller[T]]
    _consumer: Optional[StreamConsumer[T]]

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)
        self._caller = None
        self._consumer = None

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


class TypedInputStream[T](TypedConsumedStream[T]):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @property
    @abstractmethod
    def endpoint_id(self) -> int:
        pass


class TypedSplitStream[T](TypedConsumedStream[T]):
    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @abstractmethod
    def add_stream(self) -> TypedConsumedStream[T]:
        pass


class TypedStreamConsumer[T](ServiceStream, StreamConsumer[T], ABC):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment"):
        super().__init__(stream_id=stream_id, env=env)

    @property
    def stream(self) -> Stream:
        return self

    @property
    def type_name(self) -> str:
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is not None:
            return orig_type.__name__
        return genetic_type.__name__



class TypedTransformConsumedStream[T, R](TypedStream[R], StreamConsumer[T], ABC):
    _caller: Optional[Caller[R]]
    _consumer: Optional[StreamConsumer[R]]

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[R]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)
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


class TypedJoinConsumedStream[K: Hashable, T1, T2, R](TypedTransformConsumedStream[KeyValue[K, T1], R]):
    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[R]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @abstractmethod
    async def consume_right(self, value: KeyValue[K, T2]) -> None:
        pass


class TypedMultiJoinConsumedStream[K: Hashable, T, R](TypedTransformConsumedStream[KeyValue[K, T], R]):
    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[R]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @abstractmethod
    async def consume_right(self, index: int, value: KeyValue[K, Any]) -> None:
        pass


class TypedBinaryConsumedStream[T](TypedConsumedStream[T], ConsumerBinary[T], ABC):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)


class TypedBinaryKVConsumedStream[K: Hashable, V](TypedConsumedStream[KeyValue[K, V]], ConsumerBinaryKV[KeyValue[K, V]], ABC):

    def __init__(self, stream_id: int,
                 env: "ServiceExecutionEnvironment",
                 serde: TypedStreamKeyValueSerde[KeyValue[K, V]]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)


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
    def get_registered_serde(self, type_name: str) -> Optional[StreamSerializer]:
        pass

    @abstractmethod
    def register_consume_statistics(self, link_id: LinkId, statistics: ConsumeStatistics) -> None:
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
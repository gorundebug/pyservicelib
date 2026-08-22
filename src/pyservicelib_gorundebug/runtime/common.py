#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Callable, Any, get_origin, Hashable, Protocol
from typing import cast, Iterable

from ..api.models.call_semantics import CallSemantics
from .environment import ServiceEnvironment, ServiceDependency
from .config import StreamConfig, Config, LinkId
from .serde import Serializer, StreamSerializer, TypedStreamSerde, StreamKeyValueSerde
from .serde import TypedStreamKeyValueSerde, StreamSerde, Serde
from .store import Storage
from .pool import TaskPool, PriorityTaskPool
from .environment.metrics import Metrics, Int64Counter, NOOP_INT64_COUNTER
from .environment.tracing import (
    Attribute,
    Tracer,
    sampling_enabled,
    span_error,
    start_span,
    string_attr,
)
from .context import Context
from .datastruct import KeyValue
from .config import EndpointConfig, DataConnectorConfig
from .serde import BytesBuffer


class Consume[T](Protocol):

    async def consume(self, value: T) -> None:
        ...

class Consumer[T](ABC):
    @abstractmethod
    async def consume(self, value: T) -> None:
        pass




class ConsumeStatistics(ABC):

    @property
    @abstractmethod
    def count(self) -> int:
        pass


class CallerStatistics(ConsumeStatistics):

    __slots__ = ("_count",)

    def __init__(self):
        # Caller.consume() runs on the service event-loop thread. A
        # threading.Lock here serialized every graph transition while adding
        # no safety for asyncio task interleaving (there is no await in inc).
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def inc(self, amount: int = 1):
        self._count += amount


class Caller[T](Consumer[T], ABC):

    _statistics: CallerStatistics
    _source: "TypedStream[T]"
    _consumer: "StreamConsumer[T]"
    _tracer: Optional[Tracer]
    _messages_counter: Int64Counter
    _record_messages: bool
    _trace_attrs: tuple[Attribute, ...]

    def __init__(self, source: "TypedStream[T]", statistics: CallerStatistics,
                 tracer: Optional[Tracer] = None,
                 messages_counter: Optional[Int64Counter] = None):
        self._source = source
        if source.consumer is None:
            raise ValueError(f"The source stream named '{source.name}' does not have consumer in Caller constructor")
        self._consumer = source.consumer
        self._statistics = statistics
        self._tracer = tracer
        self._messages_counter = messages_counter if messages_counter is not None else NOOP_INT64_COUNTER
        self._record_messages = self._messages_counter is not NOOP_INT64_COUNTER
        self._trace_attrs = (
            string_attr("from", self._source.name),
            string_attr("to", self._consumer.stream.name),
        )

    @property
    def is_async(self) -> bool:
        return False


class DirectCaller[T](Caller[T]):

    def __init__(self, source: "TypedStream[T]", statistics: CallerStatistics,
                 tracer: Optional[Tracer] = None,
                 messages_counter: Optional[Int64Counter] = None,
                 async_: bool = False):
        super().__init__(source=source, statistics=statistics, tracer=tracer, messages_counter=messages_counter)
        self._async = async_

    async def consume(self, value: T):
        self._statistics.inc()
        if self._record_messages:
            self._messages_counter.inc()
        if self._tracer is None or not sampling_enabled():
            await self._consumer.consume(value)
            return
        _, span = start_span(self._tracer, "stream.call",
                             *self._trace_attrs)
        try:
            with span.scoped():
                await self._consumer.consume(value)
        finally:
            span.end()

    @property
    def is_async(self) -> bool:
        return self._async


class TaskPoolCaller[T](Caller[T]):
    _task_pool: TaskPool

    def __init__(self, task_pool: TaskPool, source: "TypedStream[T]", statistics: CallerStatistics,
                 tracer: Optional[Tracer] = None,
                 messages_counter: Optional[Int64Counter] = None):
        super().__init__(source=source, statistics=statistics, tracer=tracer, messages_counter=messages_counter)
        self._task_pool = task_pool
        self._trace_attrs += (
            string_attr("type", "taskpool"),
            string_attr("taskpoolname", task_pool.name),
        )

    async def consume(self, value: T):
        self._statistics.inc()
        if self._record_messages:
            self._messages_counter.inc()
        consumer = self._consumer
        if self._tracer is None or not sampling_enabled():
            async def _task_untraced():
                await consumer.consume(value)

            await self._task_pool.add_task(_task_untraced)
            return
        _, span = start_span(self._tracer, "stream.call", *self._trace_attrs)

        async def _task():
            try:
                with span.scoped():
                    await consumer.consume(value)
            except Exception as e:
                span_error(span, e)
                raise
            finally:
                span.end()

        try:
            await self._task_pool.add_task(_task)
        except Exception as e:
            span_error(span, e)
            span.end()
            raise

    @property
    def is_async(self) -> bool:
        return True


class PriorityTaskPoolCaller[T](Caller[T]):
    _priority_task_pool: PriorityTaskPool
    _priority: int

    def __init__(self, priority_task_pool: PriorityTaskPool,
                 priority: int,
                 source: "TypedStream[T]",
                 statistics: CallerStatistics,
                 tracer: Optional[Tracer] = None,
                 messages_counter: Optional[Int64Counter] = None):
        super().__init__(source=source, statistics=statistics, tracer=tracer, messages_counter=messages_counter)
        self._priority_task_pool = priority_task_pool
        self._priority = priority
        self._trace_attrs += (
            string_attr("type", "prioritytaskpool"),
            string_attr("taskpoolname", priority_task_pool.name),
        )

    async def consume(self, value: T):
        from .context import priority_from_context
        self._statistics.inc()
        if self._record_messages:
            self._messages_counter.inc()
        priority = priority_from_context()
        if priority is None:
            priority = self._priority
        consumer = self._consumer
        if self._tracer is None or not sampling_enabled():
            async def _task_untraced():
                await consumer.consume(value)

            await self._priority_task_pool.add_task(priority, _task_untraced)
            return
        _, span = start_span(self._tracer, "stream.call", *self._trace_attrs)

        async def _task():
            try:
                with span.scoped():
                    await consumer.consume(value)
            except Exception as e:
                span_error(span, e)
                raise
            finally:
                span.end()

        try:
            await self._priority_task_pool.add_task(priority, _task)
        except Exception as e:
            span_error(span, e)
            span.end()
            raise

    @property
    def is_async(self) -> bool:
        return True


class ParallelCaller[T](Caller[T]):

    def __init__(self, source: "TypedStream[T]", statistics: CallerStatistics,
                 tracer: Optional[Tracer] = None,
                 messages_counter: Optional[Int64Counter] = None):
        super().__init__(source=source, statistics=statistics, tracer=tracer, messages_counter=messages_counter)
        self._trace_attrs += (string_attr("type", "parallel"),)

    async def consume(self, value: T):
        self._statistics.inc()
        if self._record_messages:
            self._messages_counter.inc()
        consumer = self._consumer
        if self._tracer is None or not sampling_enabled():
            self._source.environment.create_task(consumer.consume, value)
            return
        _, span = start_span(self._tracer, "stream.call", *self._trace_attrs)

        async def _task():
            try:
                with span.scoped():
                    await consumer.consume(value)
            finally:
                span.end()

        self._source.environment.create_task(_task)

    @property
    def is_async(self) -> bool:
        return True


class Collect[T](ABC):

    @abstractmethod
    async def out(self, value: T) -> None:
        pass


class CollectFunc[T](Collect[T]):
    _fn: Callable

    def __init__(self, fn: Callable):
        self._fn = fn

    async def out(self, value: T) -> None:
        await self._fn(value)


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
        stream_cfg = source.config
        call_semantics: Optional[CallSemantics] = None
        if link is not None:
            if stream_cfg.id_service == service_config.id:
                call_semantics = link.call_semantics
            else:
                call_semantics = link.income_call_semantics
        if call_semantics is None or call_semantics == CallSemantics.Inherited:
            call_semantics = service_config.default_call_semantics
        if call_semantics is None or call_semantics == CallSemantics.Inherited:
            call_semantics = CallSemantics.FunctionCall

        statistics = CallerStatistics()
        # A virtual error output intentionally shares its owner's config ID.
        # Keep its counters out of the config-link map so it cannot replace
        # the normal branch statistics; status rendering tracks it separately.
        if not getattr(source, "is_error_stream", False):
            runtime.register_consume_statistics(LinkId(from_id=source.id, to_id=consumer.stream.id), statistics)
            runtime.register_link_info(RuntimeLinkInfo(from_id=source.id, to_id=consumer.stream.id, call_semantics=call_semantics))

        messages_counter = env.metrics.scope("stream", {
            "service": service_config.name,
            "from": source.name,
            "to": consumer.stream.name,
        }).counter("messages_total", "Total number of messages processed by stream link", {})

        tracing = env.tracing
        tracer = tracing.tracer(service_config.name) if tracing is not None else None

        if call_semantics == CallSemantics.FunctionCall:
            async_ = bool(
                link is not None
                and link.call_semantics == CallSemantics.FunctionCall
                and link.var_async
            )
            return DirectCaller[T](source=source, statistics=statistics, tracer=tracer,
                                   messages_counter=messages_counter, async_=async_)

        elif call_semantics == CallSemantics.TaskPool:
            if link is None:
                raise ValueError(f"TaskPool call semantics requires an explicit link config "
                                 f"for streams from={source.id} to={consumer.stream.id}")
            pool_name = link.pool_name if stream_cfg.id_service == service_config.id else link.income_pool_name
            if pool_name is None:
                raise ValueError(f"Invalid {'' if stream_cfg.id_service == service_config.id else 'income '}"
                                 f"pool name for link between streams from={source.id} to={consumer.stream.id}")

            return TaskPoolCaller[T](task_pool=runtime.get_task_pool(pool_name), source=source,
                                     statistics=statistics, tracer=tracer,
                                     messages_counter=messages_counter)

        elif call_semantics == CallSemantics.PriorityTaskPool:
            if link is None:
                raise ValueError(f"PriorityTaskPool call semantics requires an explicit link config "
                                 f"for streams from={source.id} to={consumer.stream.id}")
            pool_name = link.pool_name if stream_cfg.id_service == service_config.id else link.income_pool_name
            if pool_name is None:
                raise ValueError(f"Invalid {'' if stream_cfg.id_service == service_config.id else 'income '}"
                                 f"priority task pool name for link between streams from={source.id} to={consumer.stream.id}")

            priority = link.priority if stream_cfg.id_service == service_config.id else link.income_priority
            if priority is None:
                raise ValueError(f"Invalid {" " if stream_cfg.id_service == service_config.id else " income "} priority\
for link between streams from={source.id} to={consumer.stream.id}")

            return PriorityTaskPoolCaller[T](priority_task_pool=runtime.get_priority_task_pool(pool_name),
                                             priority=priority,
                                             source=source,
                                             statistics=statistics,
                                             tracer=tracer,
                                             messages_counter=messages_counter)

        elif call_semantics == CallSemantics.ParallelCall:
            return ParallelCaller[T](source=source, statistics=statistics, tracer=tracer,
                                     messages_counter=messages_counter)

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
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
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
    def get_endpoint(self, id_endpoint: int) -> Optional["InputEndpoint"]:
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

    @abstractmethod
    def on_missing_stream_id(self) -> None:
        pass

    @abstractmethod
    def on_late_result(self, stream_id: str) -> None:
        pass

    @abstractmethod
    def on_unknown_message_id(self, stream_id: str, message_id: str) -> None:
        pass

    @abstractmethod
    def on_duplicate_message_id(self, stream_id: str, message_id: str) -> None:
        pass

    @abstractmethod
    def on_pending_add(self, stream_id: str) -> None:
        pass

    @abstractmethod
    def on_pending_remove(self, stream_id: str) -> None:
        pass

    @abstractmethod
    def on_request_start(self) -> float:
        pass

    @abstractmethod
    def on_request_end(self, start_time: float, err: Optional[Exception]) -> None:
        pass

    @abstractmethod
    def on_invalid_http_method(self, method: str) -> None:
        pass

    @abstractmethod
    def on_begin_request_failed(self, err: Exception) -> None:
        pass


class DataSink(DataConnector):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
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
    def add_endpoint(self, endpoint: "SinkEndpoint") -> None:
        pass

    @abstractmethod
    def get_endpoint(self, id_endpoint: int) -> Optional["SinkEndpoint"]:
        pass

    @property
    @abstractmethod
    def endpoints(self) -> Iterable["SinkEndpoint"]:
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
    def environment(self) -> "ServiceExecutionEnvironment":
        pass

    @property
    @abstractmethod
    def datasink(self) -> DataSink:
        pass

    @abstractmethod
    def add_endpoint_consumer(self, consumer: OutputEndpointConsumer) -> None:
        pass

    @property
    @abstractmethod
    def endpoint_consumers(self) -> Iterable[OutputEndpointConsumer]:
        pass

    @abstractmethod
    def on_begin_request_failed(self, err: Exception) -> None:
        pass

    @abstractmethod
    def on_late_result(self, stream_id: str) -> None:
        pass

    @abstractmethod
    def on_request_start(self) -> float:
        pass

    @abstractmethod
    def on_request_end(self, start_time: float, err: Optional[Exception]) -> None:
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
    _name: str
    _transformation_name: str
    _environment: "ServiceExecutionEnvironment"

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment"):
        self._id = stream_id
        self._environment = env
        stream_config = env.config.get_stream_config_by_id(stream_id)
        self._name = stream_config.name
        self._transformation_name = stream_config.transformation_name
        env.runtime.register_stream(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def transformation_name(self) -> str:
        return self._transformation_name

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


class TypedSinkStream[T, E](ServiceStream, StreamConsumer[T], ABC):
    _serde: Optional[TypedStreamSerde[T]]

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment",
                 serde: Optional[TypedStreamSerde[T]] = None):
        super().__init__(stream_id=stream_id, env=env)
        self._serde = serde

    @property
    def stream(self) -> Stream:
        return self

    @property
    def serde(self) -> TypedStreamSerde[T]:
        if self._serde is None:
            raise ValueError("serde is not initialized for TypedSinkStream")
        return self._serde

    @property
    def type_name(self) -> str:
        genetic_type = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is not None:
            return orig_type.__name__
        return genetic_type.__name__

    @property
    @abstractmethod
    def endpoint_id(self) -> int:
        pass

    @property
    @abstractmethod
    def error_stream(self) -> "TypedConsumedStream[E]":
        pass

    @abstractmethod
    def set_sink_consumer(self, consumer: "Consumer[T]") -> None:
        pass


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
        self._consumer = value
        self._caller = RuntimeHelpers[T](self.environment).make_caller(self)

    @property
    def stream(self) -> Stream:
        return self

    @property
    def consumers(self) -> list[Stream]:
        if self._consumer is None:
            return []
        return [self._consumer.stream]


class TypedInputStream[T, R, E](TypedConsumedStream[T]):

    def __init__(self, stream_id: int, env: "ServiceExecutionEnvironment", serde: TypedStreamSerde[T]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @property
    @abstractmethod
    def endpoint_id(self) -> int:
        pass

    @property
    @abstractmethod
    def error_stream(self) -> "TypedConsumedStream[E]":
        pass

    def get_result_stream(self) -> Optional["TypedStream[R]"]:
        return None

    def set_result_consumer(self, consumer: "Consumer[R]") -> None:
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
        self._consumer = value
        self._caller = RuntimeHelpers[R](self.environment).make_caller(self)

    @property
    def stream(self) -> Stream:
        return self

    @property
    def type_name(self) -> str:
        # Concrete transform classes may add their own generic parameters
        # (Process[T, R, E], Join[K, T1, T2, R], ...), so indexing
        # __orig_class__ here does not reliably identify R. The configured
        # output serde is the canonical runtime description of the emitted
        # value type.
        return self.serde.value_serializer.type_name

    @property
    def consumers(self) -> list[Stream]:
        if self._consumer is None:
            return []
        return [self._consumer.stream]


class TypedSinkStreamWithResult[T, R, E](TypedTransformConsumedStream[T, R]):

    @property
    @abstractmethod
    def endpoint_id(self) -> int:
        pass

    @property
    @abstractmethod
    def error_stream(self) -> "TypedConsumedStream[E]":
        pass

    @abstractmethod
    def set_sink_consumer(self, consumer: "Consumer[T]") -> None:
        pass

    @abstractmethod
    async def consume_result(self, value: R) -> None:
        pass


class TypedWhenStream(Stream, ABC):
    """Non-generic when-stream interface, equivalent to Go's runtime.WhenStream (embeds Stream)."""

    @abstractmethod
    def set_index(self, index: int) -> None:
        pass

    @abstractmethod
    async def consume_case(self, value: Any) -> None:
        pass

    @abstractmethod
    def get_when_consumer(self) -> "Stream":
        pass

    @property
    @abstractmethod
    def type(self) -> type:
        pass


class TypedCaseStream[T](TypedConsumedStream[T]):

    @abstractmethod
    def add_stream(self, stream: TypedWhenStream) -> None:
        pass


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
    def get_datasource(self, id_connector: int) -> Optional[DataSource]:
        pass

    @abstractmethod
    def add_datasink(self, datasink: DataSink) -> None:
        pass

    @abstractmethod
    def get_datasink(self, id_connector: int) -> Optional[DataSink]:
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

    def has_custom_http_server(self) -> bool:
        return False

    @abstractmethod
    def register_http_handler(self, path: str, handler: Callable[..., Any], method: str = '*') -> None:
        pass

    def service_context(self) -> Any:
        return self


@dataclass(frozen=True, slots=True)
class RuntimeLinkInfo:
    from_id: int
    to_id: int
    call_semantics: CallSemantics


class RuntimeStream(ABC):

    @abstractmethod
    def build(self) -> None:
        pass

    @abstractmethod
    def get_consumers(self) -> list["Stream"]:
        pass

    @property
    @abstractmethod
    def stream(self) -> "Stream":
        pass


class RuntimeEndpointConsumer(ABC):

    @property
    @abstractmethod
    def id(self) -> int:
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
    def register_link_info(self, link_info: RuntimeLinkInfo) -> None:
        pass

    @abstractmethod
    def register_endpoint_consumer(self, consumer: RuntimeEndpointConsumer) -> None:
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
    _stream: TypedStream[T]

    def __init__(self, stream: TypedStream[T]):
        self._stream = stream

    def before_call(self):
        pass

    def after_call(self):
        pass


class Collector[T](Collect[T]):
    _caller: Optional[Caller[T]]

    def __init__(self, caller: Optional[Caller[T]]):
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


class StreamContext[T, R, E]:
    """Bundles the typed input stream, result stream, and collectors for datasource handlers."""

    stream: "TypedInputStream[T, R, E]"
    result_stream: Optional["TypedStream[R]"]
    _collect: Collect[T]
    _error_collect: Collect[E]

    def __init__(self, stream: "TypedInputStream[T, R, E]",
                 result_stream: Optional["TypedStream[R]"],
                 collect: Collect[T],
                 error_collect: Collect[E]):
        self.stream = stream
        self.result_stream = result_stream
        self._collect = collect
        self._error_collect = error_collect

    async def collect(self, value: T) -> None:
        await self._collect.out(value)

    async def error_collect(self, value: E) -> None:
        await self._error_collect.out(value)


class SinkStreamContext[T, R, E]:
    """Bundles the typed sink stream and collectors for datasink handlers."""

    stream: "TypedSinkStreamWithResult[T, R, E]"
    _collect: Collect[R]
    _error_collect: Collect[E]

    def __init__(self, stream: "TypedSinkStreamWithResult[T, R, E]",
                 collect: Collect[R],
                 error_collect: Collect[E]):
        self.stream = stream
        self._collect = collect
        self._error_collect = error_collect

    async def collect(self, value: R) -> None:
        await self._collect.out(value)

    async def error_collect(self, value: E) -> None:
        await self._error_collect.out(value)

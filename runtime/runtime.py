#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#  file for details.
#

from abc import ABC, abstractmethod
from pyservicelib.runtime import StreamExecutionEnvironment, ServiceStream
from pyservicelib.runtime.config import LinkId, Config
from pyservicelib.runtime.serde import Serializer, StreamSerializer
from pyservicelib.runtime.store import Storage
from pyservicelib.runtime.pool import TaskPool, PriorityTaskPool

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
    def _service_init(self, name: str, runtime: "StreamExecutionRuntime",  config: Config) -> None:
        pass

    @abstractmethod
    def _get_serde(self, value_type: type) -> Serializer:
        pass

    @abstractmethod
    def _register_stream(self, stream: ServiceStream) -> None:
        pass

    @abstractmethod
    def _register_serde(self, value_type: type, stream: ServiceStream) -> None:
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
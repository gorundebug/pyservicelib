#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from datetime import timedelta
import time
from typing import List
from pyservicelib.runtime.config import StreamConfig
from pyservicelib.runtime import StreamExecutionRuntime

class Context:
    __deadline: float

    def __init__(self, timeout: timedelta):
        self.__deadline = time.perf_counter() + timeout.total_seconds()

    @property
    def is_expired(self) -> bool:
        return time.perf_counter() >= self.__deadline

    @property
    def time_left(self) -> float:
        return max(0.0, self.__deadline - time.perf_counter())


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

class StreamExecutionEnvironment(ABC):

    @abstractmethod
    def get_consume_timeout(self, from_value: int, to_value: int) -> timedelta:
        pass

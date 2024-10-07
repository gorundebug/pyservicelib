#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from typing import Any

class Serializer(ABC):
    @abstractmethod
    def serialize_obj(self, obj: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize_obj(self, data: bytes) -> Any:
        pass

class StreamSerializer(ABC):

    @property
    @abstractmethod
    def is_key_value(self) -> bool:
        pass


class Serde[T](Serializer):

    @abstractmethod
    def serialize(self, obj: T) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> T:
        pass


class StreamSerde[T](StreamSerializer):

    @abstractmethod
    def serialize(self, obj: T) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> T:
        pass


class StreamKeyValueSerde[T](StreamSerde):

    @abstractmethod
    def serialize_key(self, obj: T) -> bytes:
        pass

    @abstractmethod
    def serialize_value(self, obj: T) -> bytes:
        pass

    @abstractmethod
    def deserialize_key_value(self, key_data: bytes, value_data: bytes) -> T:
        pass

    @property
    @abstractmethod
    def key_serializer(self):
        pass

    @property
    @abstractmethod
    def value_serializer(self):
        pass


class SerdeTypeHelper[T]:
    def get_type(self) -> type:
        return self.__orig_class__.__args__[0] #pyright: ignore
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import struct
from abc import ABC, abstractmethod
from typing import Any
import sys

UINT_SIZE = 4 if sys.maxsize <= 2**32 else 8

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


class SerdeHelpers[T]:
    def get_type(self) -> type:
        return self.__orig_class__.__args__[0] #pyright: ignore


class BytesSerde(Serde[bytes]):
    def serialize_obj(self, obj: bytes) -> bytes:
        if not isinstance(obj, bytes):
            raise ValueError("obj is not int")
        return self.serialize(obj)

    def deserialize_obj(self, data: bytes) -> bytes:
        return self.deserialize(data)

    def serialize(self, obj: bytes) -> bytes:
        length = len(obj)

        if UINT_SIZE == 4:
            data = bytearray(struct.pack('<I', length))
        else:
            data = bytearray(struct.pack('<Q', length))

        data.extend(obj)
        return bytes(data)

    def deserialize(self, data: bytes) -> bytes:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error BytesSerde.deserialize")

        if UINT_SIZE == 4:
            length = struct.unpack('<I', data[:4])[0]
        else:
            length = struct.unpack('<Q', data[:8])[0]

        if len(data) < UINT_SIZE + length:
            raise ValueError("deserialization error BytesSerde.deserialize: not enough data")

        return data[UINT_SIZE:UINT_SIZE + length]


class IntSerde(Serde[int]):
    def serialize_obj(self, obj: int) -> bytes:
        if not isinstance(obj, int):
            raise ValueError("obj is not int")
        return self.serialize(obj)

    def deserialize_obj(self, data: bytes) -> int:
        return self.deserialize(data)

    def serialize(self, obj: int) -> bytes:
        num_bytes = (obj.bit_length() + 7) // 8
        if num_bytes < UINT_SIZE:
            num_bytes = UINT_SIZE
        return obj.to_bytes(num_bytes, byteorder='little')

    def deserialize(self, data: bytes) -> int:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error IntSerde.deserialize")
        return int.from_bytes(data, byteorder='little')


class FloatSerde(Serde[float]):
    def serialize_obj(self, obj: float) -> bytes:
        if not isinstance(obj, float):
            raise ValueError("obj is not float")
        return self.serialize(obj)

    def deserialize_obj(self, data: bytes) -> float:
        return self.deserialize(data)

    def serialize(self, obj: float) -> bytes:
        return struct.pack('<d', obj)

    def deserialize(self, data: bytes) -> float:
        if len(data) < 8:
            raise ValueError("deserialization error FloatSerde.deserialize")
        return struct.unpack('<d', data)[0]


class BoolSerde(Serde[bool]):
    def serialize_obj(self, obj: bool) -> bytes:
        if not isinstance(obj, bool):
            raise ValueError("obj is not bool")
        return self.serialize(obj)

    def deserialize_obj(self, data: bytes) -> bool:
        return self.deserialize(data)

    def serialize(self, obj: bool) -> bytes:
        return bytes([1 if obj else 0])

    def deserialize(self, data: bytes) -> bool:
        if len(data) != 1:
            raise ValueError("deserialization error BoolSerde.deserialize: expected 1 byte")
        return data[0] == 1


class StringSerde(Serde[str]):
    def serialize_obj(self, obj: str) -> bytes:
        if not isinstance(obj, str):
            raise ValueError("obj is not string")
        return self.serialize(obj)

    def deserialize_obj(self, data: bytes) -> str:
        return self.deserialize(data)

    def serialize(self, obj: str) -> bytes:
        encoded_value = obj.encode('utf-8')
        length = len(encoded_value)

        if UINT_SIZE == 4:
            data = bytearray(struct.pack('<I', length))
        else:
            data = bytearray(struct.pack('<Q', length))

        data.extend(encoded_value)
        return bytes(data)

    def deserialize(self, data: bytes) -> str:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error StringSerde.deserialize")

        if UINT_SIZE == 4:
            length = struct.unpack('<I', data[:4])[0]
        else:
            length = struct.unpack('<Q', data[:8])[0]

        if len(data) < UINT_SIZE + length:
            raise ValueError("deserialization error StringSerde.deserialize: not enough data")

        return data[UINT_SIZE:UINT_SIZE + length].decode('utf-8')


def make_default_serde(value_type: type) -> Serializer:
    if value_type == int:
        return IntSerde()
    elif value_type == str:
        return StringSerde()
    elif value_type == bool:
        return BoolSerde()
    elif value_type == float:
        return FloatSerde()
    elif value_type == bytes:
        return BytesSerde()

    raise ValueError(f"make_default_serde unsupported type: {value_type}")
#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import struct
from abc import ABC, abstractmethod
from typing import Any, cast, Hashable
import sys

from pyservicelib.runtime.datastruct import KeyValue

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


class TypedStreamSerde[T](StreamSerializer):

    @abstractmethod
    def serialize(self, obj: T) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> T:
       pass


class TypedStreamKeyValueSerde[T](TypedStreamSerde[T]):

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
    def key_serializer(self) -> Serializer:
        pass

    @property
    @abstractmethod
    def value_serializer(self) -> Serializer:
        pass



class StreamSerde[T](TypedStreamSerde[T]):
    _serde: Serde[T]

    def __init__(self, serde: Serde[T]):
        super().__init__()
        self._serde = serde

    @property
    def is_key_value(self) -> bool:
        return False

    def serialize(self, obj: T) -> bytes:
        return self._serde.serialize(obj)

    def deserialize(self, data: bytes) -> T:
        return self._serde.deserialize(data)


class StreamKeyValueSerde[K: Hashable, V](TypedStreamKeyValueSerde[KeyValue[K, V]]):
    _serde_key: Serde[K]
    _serde_value: Serde[V]

    def __init__(self, serde_key: Serde[K], serde_value: Serde[V]):
        super().__init__()
        self._serde_key = serde_key
        self._serde_value = serde_value

    @property
    def is_key_value(self) -> bool:
        return True

    def serialize(self, obj: KeyValue[K, V]) -> bytes:

        key_bytes = self._serde_key.serialize(obj.key)
        key_len_bytes = struct.pack('<I' if UINT_SIZE == 4 else '<Q', len(key_bytes))

        value_bytes = self._serde_value.serialize(obj.value)
        value_len_bytes = struct.pack('<I' if UINT_SIZE == 4 else '<Q', len(value_bytes))

        data = bytearray(len(key_len_bytes) + len(key_bytes) + len(value_len_bytes) + len(value_bytes))

        offset = 0
        data[offset:offset + len(key_len_bytes)] = key_len_bytes
        offset += len(key_len_bytes)
        data[offset:offset + len(key_bytes)] = key_bytes
        offset += len(key_bytes)
        data[offset:offset + len(value_len_bytes)] = value_len_bytes
        offset += len(value_len_bytes)
        data[offset:offset + len(value_bytes)] = value_bytes

        return bytes(data)

    def deserialize(self, data: bytes) -> KeyValue[K, V]:
        data_view = memoryview(data)

        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialize key len error streamKeyValueSerde")

        key_len = struct.unpack('<I' if UINT_SIZE == 4 else '<Q', data_view[:UINT_SIZE])[0]
        data_view = data_view[UINT_SIZE:]

        if len(data_view) < key_len:
            raise ValueError("deserialize key error streamKeyValueSerde")

        key = self._serde_key.deserialize(cast(bytes, data_view[:key_len]))
        data_view = data_view[key_len:]

        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialize value len error streamKeyValueSerde")

        value_len = struct.unpack('<I' if UINT_SIZE == 4 else '<Q', data_view[:UINT_SIZE])[0]
        data_view = data_view[UINT_SIZE:]

        if len(data_view) < value_len:
            raise ValueError("deserialize value error streamKeyValueSerde")

        value = self._serde_value.deserialize(cast(bytes, data_view[:value_len]))
        return KeyValue[K, V](key=key, value=value)

    def serialize_key(self, obj: KeyValue[K, V]) -> bytes:
        return self._serde_key.serialize(obj.key)

    def serialize_value(self, obj: KeyValue[K, V]) -> bytes:
        return self._serde_value.serialize(obj.value)

    def deserialize_key_value(self, key_data: bytes, value_data: bytes) -> KeyValue[K, V]:
        return KeyValue[K, V](self._serde_key.deserialize(key_data), self._serde_value.deserialize(value_data))

    @property
    def key_serializer(self) -> Serializer:
        return self._serde_key

    @property
    def value_serializer(self) -> Serializer:
        return self._serde_value


class SerdeHelpers[T]:
    def get_type(self) -> type:
        return self.__orig_class__.__args__[0] #pyright: ignore


class BytesSerde(Serde[bytes]):
    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise ValueError("obj is not int")
        return self.serialize(cast(bytes, obj))

    def deserialize_obj(self, data: bytes) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: bytes) -> bytes:
        length = len(obj)
        data = bytearray(length + UINT_SIZE)

        if UINT_SIZE == 4:
            data[:UINT_SIZE] = struct.pack('<I', length)
        else:
            data[:UINT_SIZE] = struct.pack('<Q', length)

        data[UINT_SIZE:UINT_SIZE + length] = obj
        return bytes(data)

    def deserialize(self, data: bytes) -> bytes:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error BytesSerde.deserialize")

        if UINT_SIZE == 4:
            length = struct.unpack('<I', data)[0]
        else:
            length = struct.unpack('<Q', data)[0]

        if len(data) < UINT_SIZE + length:
            raise ValueError("deserialization error BytesSerde.deserialize: not enough data")

        return data[UINT_SIZE:UINT_SIZE + length]


class IntSerde(Serde[int]):
    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, int):
            raise ValueError("obj is not int")
        return self.serialize(cast(int, obj))

    def deserialize_obj(self, data: bytes) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: int) -> bytes:
        num_bytes = (-obj).bit_length() // 8 + 1 if obj < 0 else (obj.bit_length() + 7) // 8
        if num_bytes < UINT_SIZE:
            num_bytes = UINT_SIZE
        return obj.to_bytes(num_bytes, byteorder='little', signed=True)

    def deserialize(self, data: bytes) -> int:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error IntSerde.deserialize")
        return int.from_bytes(data, byteorder='little', signed=True)


class FloatSerde(Serde[float]):
    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, float):
            raise ValueError("obj is not float")
        return self.serialize(cast(int, obj))

    def deserialize_obj(self, data: bytes) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: float) -> bytes:
        return struct.pack('<d', obj)

    def deserialize(self, data: bytes) -> float:
        if len(data) < 8:
            raise ValueError("deserialization error FloatSerde.deserialize")
        return struct.unpack('<d', data)[0]


class BoolSerde(Serde[bool]):
    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, bool):
            raise ValueError("obj is not bool")
        return self.serialize(cast(bool, obj))

    def deserialize_obj(self, data: bytes) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: bool) -> bytes:
        return bytes([1 if obj else 0])

    def deserialize(self, data: bytes) -> bool:
        if len(data) != 1:
            raise ValueError("deserialization error BoolSerde.deserialize: expected 1 byte")
        return data[0] == 1


class StringSerde(Serde[str]):

    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, str):
            raise ValueError("obj is not string")
        return self.serialize(cast(str, obj))

    def deserialize_obj(self, data: bytes) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: str) -> bytes:
        encoded_value = obj.encode('utf-8')
        length = len(encoded_value)

        data = bytearray(length + UINT_SIZE)
        if UINT_SIZE == 4:
            data[:UINT_SIZE] = struct.pack('<I', length)
        else:
            data[:UINT_SIZE] = struct.pack('<Q', length)

        data[UINT_SIZE:UINT_SIZE + length] = encoded_value
        return bytes(data)

    def deserialize(self, data: bytes) -> str:
        data_view = memoryview(data)
        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialization error StringSerde.deserialize")

        if UINT_SIZE == 4:
            length = struct.unpack('<I', data_view)[0]
        else:
            length = struct.unpack('<Q', data_view)[0]

        if len(data_view) < UINT_SIZE + length:
            raise ValueError("deserialization error StringSerde.deserialize: not enough data")

        return data_view[UINT_SIZE:UINT_SIZE + length].cast('B').tobytes().decode('utf-8')


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
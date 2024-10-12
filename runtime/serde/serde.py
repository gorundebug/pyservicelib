#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import struct
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, cast, Hashable, Optional, Dict
import sys

from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.api.models.data_type import DataType

UINT_SIZE = 4 if sys.maxsize <= 2**32 else 8
PACK_FORMAT = '<I' if UINT_SIZE == 4 else '<Q'

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
        key_len_bytes = struct.pack(PACK_FORMAT, len(key_bytes))

        value_bytes = self._serde_value.serialize(obj.value)
        value_len_bytes = struct.pack(PACK_FORMAT, len(value_bytes))

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

        key_len = struct.unpack(PACK_FORMAT, data_view[:UINT_SIZE])[0]
        data_view = data_view[UINT_SIZE:]

        if len(data_view) < key_len:
            raise ValueError("deserialize key error streamKeyValueSerde")

        key = self._serde_key.deserialize(cast(bytes, data_view[:key_len]))
        data_view = data_view[key_len:]

        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialize value len error streamKeyValueSerde")

        value_len = struct.unpack(PACK_FORMAT, data_view[:UINT_SIZE])[0]
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
        data[:UINT_SIZE] = struct.pack(PACK_FORMAT, length)
        data[UINT_SIZE:UINT_SIZE + length] = obj
        return bytes(data)

    def deserialize(self, data: bytes) -> bytes:
        if len(data) < UINT_SIZE:
            raise ValueError("deserialization error BytesSerde.deserialize")

        length = struct.unpack(PACK_FORMAT, data)[0]

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
        data[:UINT_SIZE] = struct.pack(PACK_FORMAT, length)
        data[UINT_SIZE:UINT_SIZE + length] = encoded_value
        return bytes(data)

    def deserialize(self, data: bytes) -> str:
        data_view = memoryview(data)
        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialization error StringSerde.deserialize")

        length = struct.unpack(PACK_FORMAT, data_view)[0]

        if len(data_view) < UINT_SIZE + length:
            raise ValueError("deserialization error StringSerde.deserialize: not enough data")

        return data_view[UINT_SIZE:UINT_SIZE + length].tobytes().decode('utf-8')


class ListSerde(Serde[List[Any]]):
    _list_type: type
    _value_serde: Serializer

    def __init__(self, list_type: type, value_serde: Serializer):
        if list_type is not list:
            raise ValueError(f"list_type is not list type {list_type.__name__}")

        self._list_type = list_type
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, self._list_type):
            raise ValueError(f"value is not of type {self._list_type.__name__}")

        list_value: List[Any] = cast(List[Any], obj)

        result = bytearray()
        result.extend(struct.pack(PACK_FORMAT, len(list_value)))

        for element in list_value:
            element_bytes = self._value_serde.serialize_obj(element)
            result.extend(struct.pack(PACK_FORMAT, len(element_bytes)))
            result.extend(element_bytes)

        return bytes(result)

    def deserialize_obj(self, data: bytes) -> Any:
        data_view = memoryview(data)
        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialization error ListSerde.deserialize invalid data size")

        count = struct.unpack(PACK_FORMAT, data_view)[0]
        data_view = data_view[UINT_SIZE:]

        result: List[Optional[Any]] = [None] * count

        for i in range(count):
            if len(data_view) < UINT_SIZE:
                raise ValueError("DeserializeObj ListSerde error (invalid element length data)")

            element_length = struct.unpack(PACK_FORMAT, data_view[:UINT_SIZE])[0]
            data_view = data_view[UINT_SIZE:]

            if len(data_view) < element_length:
                raise ValueError("DeserializeObj ListSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(cast(bytes, data_view[:element_length]))
            result[i] = element

            data_view = data_view[element_length:]

        return result

    def serialize(self, obj: List[Any]) -> bytes:
        return self.serialize_obj(obj)

    def deserialize(self, data: bytes) -> List[Any]:
        return cast(List[Any], self.deserialize_obj(data))


class TupleSerde(Serde[Tuple[Any, ...]]):
    _tuple_type: type
    _value_serde: Serializer

    def __init__(self, tuple_type: type, value_serde: Serializer):
        if tuple_type is not tuple:
            raise ValueError(f"tuple_type is not list type {tuple_type.__name__}")

        self._tuple_type = tuple_type
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, self._tuple_type):
            raise ValueError(f"value is not of type {self._tuple_type.__name__}")

        tuple_value: Tuple[Any, ...] = cast(Tuple[Any, ...], obj)

        result = bytearray()
        result.extend(struct.pack(PACK_FORMAT, len(tuple_value)))

        for element in tuple_value:
            element_bytes = self._value_serde.serialize_obj(element)
            result.extend(struct.pack(PACK_FORMAT, len(element_bytes)))
            result.extend(element_bytes)

        return bytes(result)

    def deserialize_obj(self, data: bytes) -> Any:
        data_view = memoryview(data)
        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialization error TupleSerde.deserialize invalid data size")

        count = struct.unpack(PACK_FORMAT, data_view)[0]
        data_view = data_view[UINT_SIZE:]

        result: List[Optional[Any]] = [None] * count

        for i in range(count):
            if len(data_view) < UINT_SIZE:
                raise ValueError("DeserializeObj TupleSerde error (invalid element length data)")

            element_length = struct.unpack(PACK_FORMAT, data_view[:UINT_SIZE])[0]
            data_view = data_view[UINT_SIZE:]

            if len(data_view) < element_length:
                raise ValueError("DeserializeObj TupleSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(cast(bytes, data_view[:element_length]))
            result[i] = element

            data_view = data_view[element_length:]

        return tuple(result)

    def serialize(self, obj: Tuple[Any, ...]) -> bytes:
        return self.serialize_obj(obj)

    def deserialize(self, data: bytes) -> Tuple[Any, ...]:
        return cast(Tuple[Any, ...], self.deserialize_obj(data))


class DictSerde(Serde[Dict[Any, Any]]):
    _dict_type: type
    _key_serde: Serializer
    _value_serde: Serializer

    def __init__(self, dict_type: type, key_serde: Serde[Any], value_serde: Serde[Any]):
        if dict_type is not tuple:
            raise ValueError(f"dict_type is not dict type {dict_type.__name__}")

        self._dict_type = dict_type
        self._key_serde = key_serde
        self._value_serde = value_serde

    def serialize(self, obj: Dict[Any, Any]) -> bytes:
        return self.serialize_obj(obj)

    def deserialize(self, data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], self.deserialize_obj(data))

    def serialize_obj(self, obj: Any) -> bytes:
        if not isinstance(obj, self._dict_type):
            raise ValueError(f"value is not of type {self._dict_type.__name__}")

        dict_value: Dict[Any, Any] = cast(Dict[Any, Any], obj)
        result = bytearray()
        result.extend(struct.pack(PACK_FORMAT, len(dict_value)))

        for key, val in dict_value.items():
            key_bytes = self._key_serde.serialize_obj(key)
            result.extend(struct.pack(PACK_FORMAT, len(key_bytes)))
            result.extend(key_bytes)

            value_bytes = self._value_serde.serialize_obj(val)
            result.extend(struct.pack(PACK_FORMAT, len(value_bytes)))
            result.extend(value_bytes)

        return bytes(result)

    def deserialize_obj(self, data: bytes) -> Any:
        data_view = memoryview(data)

        if len(data_view) < UINT_SIZE:
            raise ValueError("deserialization error DictSerde.deserialize invalid data size")

        count = struct.unpack_from(PACK_FORMAT, data_view)[0]
        data_view = data_view[UINT_SIZE:]

        result: Dict[Any, Any] = {}
        for _ in range(count):
            if len(data_view) < UINT_SIZE:
                raise ValueError("DeserializeObj DictSerde error (invalid key length data)")

            key_length = struct.unpack(PACK_FORMAT, data_view[:UINT_SIZE])[0]
            data_view = data_view[UINT_SIZE:]

            if len(data_view) < key_length:
                raise ValueError("DeserializeObj DictSerde error (invalid key data)")

            key = self._key_serde.deserialize_obj(cast(bytes, data_view[:key_length]))
            data_view = data_view[key_length:]

            if len(data_view) < UINT_SIZE:
                raise ValueError("DeserializeObj DictSerde error (invalid value length data)")

            value_len = struct.unpack_from(PACK_FORMAT, data_view[:UINT_SIZE])[0]
            data_view = data_view[UINT_SIZE:]

            if len(data_view) < key_length:
                raise ValueError("DeserializeObj DictSerde error (invalid value data)")

            value = self._value_serde.deserialize_obj(cast(bytes, data_view[:value_len]))
            data_view = data_view[value_len:]

            result[key] = value

        return result

def python_type_by_type(type_name: str) -> Optional[str]:
    type_mapping = {
        DataType.INT: 'int',
        DataType.UINT: 'int',
        DataType.BYTE: 'int',
        DataType.CHAR: 'int',
        DataType.BOOLEAN: 'bool',
        DataType.UNICODE_CHAR: 'str',
        DataType.STRING: 'str',
        DataType.UNICODE_STRING: 'str',
        DataType.FLOAT: 'float,',
        DataType.DOUBLE: 'float',
        DataType.INT8: 'int',
        DataType.INT16: 'int',
        DataType.INT32: 'int',
        DataType.INT64: 'int',
        DataType.UINT8: 'int',
        DataType.UINT16: 'int',
        DataType.UINT32: 'int',
        DataType.UINT64: 'int'
    }
    return type_mapping.get(cast(DataType, type_name), None)


def make_default_serde(type_name: str) -> Serializer:
    if type_name == 'int':
        return IntSerde()
    elif type_name == 'str':
        return StringSerde()
    elif type_name == 'bool':
        return BoolSerde()
    elif type_name == 'float':
        return FloatSerde()
    elif type_name == 'bytes':
        return BytesSerde()

    raise ValueError(f"make_default_serde unsupported type: {type_name}")

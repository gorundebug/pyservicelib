#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import struct
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, cast, Hashable, Optional, Dict, Union
import sys

from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.api.models.data_type import DataType

BytesBuffer = Union[bytes, bytearray, memoryview]


UINT_SIZE = 4 if sys.maxsize <= 2**32 else 8
MAX_SIZE_LENGTH = UINT_SIZE
PACK_FORMAT = '<I' if UINT_SIZE == 4 else '<Q'


def set_size(b: Union[bytearray, memoryview], offset: int, size: int) -> int:
    struct.pack_into(PACK_FORMAT, b, offset, size)
    return UINT_SIZE


def get_size(b: Union[bytearray, memoryview], offset: int) -> Tuple[int, int]:
    if len(b) < UINT_SIZE:
        raise ValueError("get_size: size length error")
    return struct.unpack_from(PACK_FORMAT, b, offset)[0], UINT_SIZE


class Serializer(ABC):
    @abstractmethod
    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        pass

    @abstractmethod
    def deserialize_obj(self, data: BytesBuffer) -> Any:
        pass

    @property
    def is_stub(self) -> bool:
        return False

class StreamSerializer(ABC):

    @property
    @abstractmethod
    def is_key_value(self) -> bool:
        pass


class Serde[T](Serializer):

    @abstractmethod
    def serialize(self, obj: T, b: BytesBuffer) -> bytearray:
        pass

    @abstractmethod
    def deserialize(self, data: BytesBuffer) -> T:
        pass


class TypedStreamSerde[T](StreamSerializer):

    @abstractmethod
    def serialize(self, obj: T) -> bytearray:
        pass

    @abstractmethod
    def deserialize(self, data: BytesBuffer) -> T:
       pass


class TypedStreamKeyValueSerde[T](TypedStreamSerde[T]):

    @abstractmethod
    def serialize_key(self, obj: T) -> bytearray:
        pass

    @abstractmethod
    def serialize_value(self, obj: T) -> bytearray:
        pass

    @abstractmethod
    def deserialize_key_value(self,
                              key_data: BytesBuffer,
                              value_data: BytesBuffer) -> T:
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

    def serialize(self, obj: T) -> bytearray:
        return self._serde.serialize(obj, bytearray())

    def deserialize(self, data: BytesBuffer) -> T:
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

    def serialize(self, obj: KeyValue[K, V]) -> bytearray:

        b = bytearray(MAX_SIZE_LENGTH)
        b = self._serde_key.serialize(obj.key, b)
        key_bytes_length = len(b) - MAX_SIZE_LENGTH
        n = set_size(b, 0, key_bytes_length)
        if n != MAX_SIZE_LENGTH:
            b[n:n + key_bytes_length] = b[MAX_SIZE_LENGTH:MAX_SIZE_LENGTH + key_bytes_length]
            del b[n + key_bytes_length:]

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        b = self._serde_value.serialize(obj.value, b)
        value_bytes_length = len(b) - b_length - MAX_SIZE_LENGTH
        n = set_size(b, b_length, value_bytes_length)
        if n != MAX_SIZE_LENGTH:
            b[b_length + n:b_length + n + value_bytes_length] = b[b_length + MAX_SIZE_LENGTH:b_length +
                                                                                             MAX_SIZE_LENGTH +
                                                                                             value_bytes_length]
            del b[b_length + n + value_bytes_length:]

        return b

    def deserialize(self, data: BytesBuffer) -> KeyValue[K, V]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        key_len, n = get_size(data, 0)
        data = data[n:]

        if len(data) < key_len:
            raise ValueError("streamKeyValueSerde: deserialize key error")

        key = self._serde_key.deserialize(data[:key_len])
        data = data[key_len:]

        value_len, n = get_size(data, 0)
        data = data[n:]

        if len(data) < value_len:
            raise ValueError("streamKeyValueSerde: deserialize value error")

        value = self._serde_value.deserialize(data[:value_len])
        return KeyValue[K, V](key=key, value=value)

    def serialize_key(self, obj: KeyValue[K, V]) -> bytearray:
        return self._serde_key.serialize(obj.key, bytearray())

    def serialize_value(self, obj: KeyValue[K, V]) -> bytearray:
        return self._serde_value.serialize(obj.value, bytearray())

    def deserialize_key_value(self, key_data: BytesBuffer, value_data: BytesBuffer) -> KeyValue[K, V]:
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
    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, bytes):
            raise ValueError("BytesSerde: obj is not bytes")
        return self.serialize(cast(bytes, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: bytes, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, len(obj))
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(obj)
        return b

    def deserialize(self, data: BytesBuffer) -> bytes:
        if not isinstance(data, memoryview):
            data = memoryview(data)
        length, n = get_size(data, 0)
        data = data[n:]
        if len(data) < length:
            raise ValueError("BytesSerde: deserialization error: not enough data")
        return data.tobytes()


class NumberSerde(Serde[int]):
    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, int):
            raise ValueError("obj is not int")
        return self.serialize(cast(int, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: int, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        num_bytes = (-obj).bit_length() // 8 + 1 if obj < 0 else (obj.bit_length() + 7) // 8
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, num_bytes)
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(obj.to_bytes(num_bytes, byteorder='little', signed=True))
        return b

    def deserialize(self, data: BytesBuffer) -> int:
        if not isinstance(data, memoryview):
            data = memoryview(data)
        length, n = get_size(data, 0)
        data = data[n:]
        if len(data) < length:
            raise ValueError("NumberSerde: deserialization error: not enough data")
        return int.from_bytes(data, byteorder='little', signed=True)


class StubSerde(Serde[Any]):
    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        return self.serialize(obj, b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        return b

    def deserialize(self, data: BytesBuffer) -> Any:
        return None


class IntSerde(Serde[int]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        if type_name == 'int':
            self._fmt = '<i' if UINT_SIZE == 4 else '<q'
            self._size = UINT_SIZE
        elif type_name == 'uint':
            self._fmt = '<I' if UINT_SIZE == 4 else '<Q'
            self._size = UINT_SIZE
        elif type_name == 'int8':
            self._fmt = '<b'
            self._size = 1
        elif type_name == 'uint8':
            self._fmt = '<B'
            self._size = 1
        elif type_name == 'int16':
            self._fmt = '<h'
            self._size = 2
        elif type_name == 'uint16':
            self._fmt = '<H'
            self._size = 2
        elif type_name == 'int32':
            self._fmt = '<i'
            self._size = 4
        elif type_name == 'uint32':
            self._fmt = '<I'
            self._size = 4
        elif type_name == 'int64':
            self._fmt = '<q'
            self._size = 8
        elif type_name == 'uint64':
            self._fmt = '<Q'
            self._size = 8
        else:
            raise ValueError(f"IntSerde: invalid type {type_name}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, int):
            raise ValueError("IntSerde: obj is not int")
        return self.serialize(cast(int, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: int, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        b.extend(struct.pack(self._fmt, obj))
        return b

    def deserialize(self, data: BytesBuffer) -> int:
        if len(data) < self._size:
            raise ValueError("IntSerde deserialization error")
        return struct.unpack(self._fmt, data)[0]


class FloatSerde(Serde[float]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        if type_name == 'float32':
            self._fmt = '<f'
            self._size = 4
        elif type_name == 'float64':
            self._fmt = '<d'
            self._size = 8
        else:
            raise ValueError(f"FloatSerde: invalid type {type_name}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, float):
            raise ValueError("FloatSerde: obj is not float")
        return self.serialize(cast(float, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: float, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        b.extend(struct.pack(self._fmt, obj))
        return b

    def deserialize(self, data: BytesBuffer) -> float:
        if len(data) < self._size:
            raise ValueError("FloatSerde: deserialization error")
        return struct.unpack(self._fmt, data)[0]


class BoolSerde(Serde[bool]):

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, bool):
            raise ValueError("BoolSerde: obj is not bool")
        return self.serialize(cast(bool, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: bool, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        b.extend([1 if obj else 0])
        return b

    def deserialize(self, data: BytesBuffer) -> bool:
        if len(data) < 1:
            raise ValueError("BoolSerde: deserialization error")
        return data[0] != 0


class StringSerde(Serde[str]):

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, str):
            raise ValueError("StringSerde: obj is not string")
        return self.serialize(cast(str, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: str, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        encoded_value = obj.encode('utf-8')
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, len(encoded_value))
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(encoded_value)
        return b

    def deserialize(self, data: BytesBuffer) -> str:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        length, n = get_size(data, 0)
        data = data[n:]
        if len(data) < length:
            raise ValueError("StringSerde: deserialization error: not enough data")
        return data[:length].tobytes().decode('utf-8')


class IntListSerde(Serde[List[int]]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        if type_name == '[]int':
            self._fmt = 'i' if UINT_SIZE == 4 else 'q'
            self._size = UINT_SIZE
        elif type_name == '[]uint':
            self._fmt = 'I' if UINT_SIZE == 4 else 'Q'
            self._size = UINT_SIZE
        elif type_name == '[]int8':
            self._fmt = 'b'
            self._size = 1
        elif type_name == '[]uint8':
            self._fmt = 'B'
            self._size = 1
        elif type_name == '[]int16':
            self._fmt = 'h'
            self._size = 2
        elif type_name == '[]uint16':
            self._fmt = 'H'
            self._size = 2
        elif type_name == '[]int32':
            self._fmt = 'i'
            self._size = 4
        elif type_name == '[]uint32':
            self._fmt = 'I'
            self._size = 4
        elif type_name == '[]int64':
            self._fmt = 'q'
            self._size = 8
        elif type_name == '[]uint64':
            self._fmt = 'Q'
            self._size = 8
        else:
            raise ValueError(f"IntListSerde: invalid type {type_name}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"IntListSerde: obj is not list")
        return self.serialize(cast(List[int], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize_obj(data)

    def serialize(self, obj: List[int], b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        count = len(obj)
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, count)
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(bytearray(self._size * count))
        n += b_length
        struct.pack_into(f'<{count}{self._fmt}', b, n, *obj)
        return b

    def deserialize(self, data: BytesBuffer) -> List[int]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("IntListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}{self._fmt}', data, n))


class FloatListSerde(Serde[List[float]]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        if type_name == '[]float32':
            self._fmt = 'f'
            self._size = 4
        elif type_name == '[]float64':
            self._fmt = 'd'
            self._size = 8
        else:
            raise ValueError(f"FloatListSerde: invalid type {type_name}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"FloatListSerde: obj is not list")
        return self.serialize(cast(List[float], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize_obj(data)

    def serialize(self, obj: List[float], b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        count = len(obj)
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, count)
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(bytearray(self._size * count))
        n += b_length
        struct.pack_into(f'<{count}{self._fmt}', b, n, *obj)
        return b

    def deserialize(self, data: BytesBuffer) -> List[float]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("FloatListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}{self._fmt}', data, n))


class BoolListSerde(Serde[List[bool]]):

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"BoolListSerde: obj is not list")
        return self.serialize(cast(List[bool], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize_obj(data)

    def serialize(self, obj: List[bool], b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        count = len(obj)
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, count)
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]
        b.extend(bytearray(count))
        n += b_length
        struct.pack_into(f'<{count}?', b, n, *obj)
        return b

    def deserialize(self, data: BytesBuffer) -> List[bool]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count + n:
            raise ValueError("BoolListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}?', data, n))


class StringListSerde(Serde[List[str]]):

    def __init__(self):
        self._value_serde = StringSerde()

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"StringListSerde: obj is not list")
        return self.serialize(cast(List[str], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize_obj(data)

    def serialize(self, obj: List[str], b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)

        count = len(obj)
        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, count)
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]

        for v in obj:
            encoded_value = v.encode('utf-8')
            b_length = len(b)
            b.extend(bytearray(MAX_SIZE_LENGTH))
            n = set_size(b, b_length, len(encoded_value))
            if n != MAX_SIZE_LENGTH:
                del b[b_length + n:]
            b.extend(encoded_value)

        return b

    def deserialize(self, data: BytesBuffer) -> List[str]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        data = data[n:]
        result = [''] * count

        for i in range(count):
            length, n = get_size(data, 0)
            data = data[n:]

            if len(data) < length:
                raise ValueError("StringListSerde: deserialization error: not enough data")

            result[i] = data[:length].tobytes().decode('utf-8')
            data = data[length:]

        return result


class ListSerde(Serde[List[Any]]):
    _list_type: type
    _value_serde: Serializer

    def __init__(self, list_type: type, value_serde: Serializer):
        if list_type is not list:
            raise ValueError(f"list_type is not list type {list_type.__name__}")

        self._list_type = list_type
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, self._list_type):
            raise ValueError(f"value is not of type {self._list_type.__name__}")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        list_value: List[Any] = cast(List[Any], obj)

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, len(list_value))
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]

        for element in list_value:
            b_length = len(b)
            b.extend(bytearray(MAX_SIZE_LENGTH))
            b = self._value_serde.serialize_obj(element, b)
            value_bytes_length = len(b) - b_length - MAX_SIZE_LENGTH
            n = set_size(b, b_length, value_bytes_length)
            if n != MAX_SIZE_LENGTH:
                b[b_length + n:b_length + n + value_bytes_length] = b[b_length +
                                                                      MAX_SIZE_LENGTH:b_length +
                                                                                      MAX_SIZE_LENGTH +
                                                                                      value_bytes_length]
                del b[b_length + n + value_bytes_length:]

        return b

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        data = data[n:]

        result: List[Optional[Any]] = [None] * count

        for i in range(count):
            element_length, n = get_size(data, 0)
            data = data[n:]

            if len(data) < element_length:
                raise ValueError("DeserializeObj ListSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(data[:element_length])
            result[i] = element

            data = data[element_length:]

        return result

    def serialize(self, obj: List[Any], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> List[Any]:
        return cast(List[Any], self.deserialize_obj(data))


class TupleSerde(Serde[Tuple[Any, ...]]):
    _tuple_type: type
    _value_serde: Serializer

    def __init__(self, tuple_type: type, value_serde: Serializer):
        if tuple_type is not tuple:
            raise ValueError(f"tuple_type is not list type {tuple_type.__name__}")

        self._tuple_type = tuple_type
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, self._tuple_type):
            raise ValueError(f"value is not of type {self._tuple_type.__name__}")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        tuple_value: Tuple[Any, ...] = cast(Tuple[Any, ...], obj)

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        n = set_size(b, b_length, len(tuple_value))
        if n != MAX_SIZE_LENGTH:
            del b[b_length + n:]

        for element in tuple_value:
            b_length = len(b)
            b.extend(bytearray(MAX_SIZE_LENGTH))
            b = self._value_serde.serialize_obj(element, b)
            value_bytes_length = len(b) - b_length - MAX_SIZE_LENGTH
            n = set_size(b, b_length, value_bytes_length)
            if n != MAX_SIZE_LENGTH:
                b[b_length + n:b_length + n + value_bytes_length] = b[b_length +
                                                                      MAX_SIZE_LENGTH:b_length +
                                                                                      MAX_SIZE_LENGTH +
                                                                                      value_bytes_length]
                del b[b_length + n + value_bytes_length:]

        return b


    def deserialize_obj(self, data: BytesBuffer) -> Any:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        data = data[n:]

        result: List[Optional[Any]] = [None] * count

        for i in range(count):
            element_length, n = get_size(data, 0)
            data = data[n:]

            if len(data) < element_length:
                raise ValueError("DeserializeObj ListSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(data[:element_length])
            result[i] = element

            data = data[element_length:]

        return tuple(result)

    def serialize(self, obj: Tuple[Any, ...], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> Tuple[Any, ...]:
        return cast(Tuple[Any, ...], self.deserialize_obj(data))


class DictSerde(Serde[Dict[Any, Any]]):
    _dict_type: type
    _keys_serde: Serializer
    _values_serde: Serializer

    def __init__(self, dict_type: type, keys_serde: Serde[Any], values_serde: Serde[Any]):
        if dict_type is not tuple:
            raise ValueError(f"dict_type is not dict type {dict_type.__name__}")

        self._dict_type = dict_type
        self._keys_serde = keys_serde
        self._values_serde = values_serde

    def serialize(self, obj: Dict[Any, Any], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], self.deserialize_obj(data))

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, self._dict_type):
            raise ValueError(f"value is not of type {self._dict_type.__name__}")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        dict_value: Dict[Any, Any] = cast(Dict[Any, Any], obj)

        keys: List[Any] = list(dict_value.keys())
        values: List[Any] = list(dict_value.values())

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        b = self._keys_serde.serialize_obj(keys, b)
        keys_bytes_length = len(b) - b_length - MAX_SIZE_LENGTH
        n = set_size(b, b_length, keys_bytes_length)
        if n != MAX_SIZE_LENGTH:
            b[b_length + n:b_length + n + keys_bytes_length] = b[b_length + MAX_SIZE_LENGTH:b_length +
                                                                                            MAX_SIZE_LENGTH +
                                                                                            keys_bytes_length]
            del b[b_length + n + keys_bytes_length:]

        b_length = len(b)
        b.extend(bytearray(MAX_SIZE_LENGTH))
        b = self._values_serde.serialize_obj(values, b)
        values_bytes_length = len(b) - b_length - MAX_SIZE_LENGTH
        n = set_size(b, b_length, values_bytes_length)
        if n != MAX_SIZE_LENGTH:
            b[b_length + n:b_length + n + values_bytes_length] = b[b_length + MAX_SIZE_LENGTH:b_length +
                                                                                              MAX_SIZE_LENGTH +
                                                                                              values_bytes_length]
            del b[b_length + n + values_bytes_length:]

        return b

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        keys_len, n = get_size(data, 0)
        data = data[n:]

        if len(data) < keys_len:
            raise ValueError("DictSerde: deserialize keys error")

        keys = self._keys_serde.deserialize_obj(data[:keys_len])
        data = data[keys_len:]

        values_len, n = get_size(data, 0)
        data = data[n:]

        if len(data) < values_len:
            raise ValueError("DictSerde: deserialize values error")

        values = self._values_serde.deserialize_obj(data[:values_len])

        return dict(zip(keys, values))

def python_type_by_type(type_name: str) -> Optional[str]:
    type_mapping = {
        DataType.INT: 'int',
        DataType.UINT: 'uint',
        DataType.BYTE: 'int8',
        DataType.CHAR: 'int32',
        DataType.BOOLEAN: 'bool',
        DataType.UNICODE_CHAR: 'str',
        DataType.STRING: 'str',
        DataType.UNICODE_STRING: 'str',
        DataType.FLOAT: 'float32,',
        DataType.DOUBLE: 'float64',
        DataType.INT8: 'int8',
        DataType.INT16: 'int16',
        DataType.INT32: 'int32',
        DataType.INT64: 'int64',
        DataType.UINT8: 'uint8',
        DataType.UINT16: 'uint16',
        DataType.UINT32: 'uint32',
        DataType.UINT64: 'uint64'
    }
    return type_mapping.get(cast(DataType, type_name), None)


def make_default_serde(type_name: str) -> Serializer:
    if type_name == 'int':
        return IntSerde(type_name)
    elif type_name == 'uint':
        return IntSerde(type_name)
    elif type_name == 'int8':
        return IntSerde(type_name)
    elif type_name == 'uint8':
        return IntSerde(type_name)
    elif type_name == 'int16':
        return IntSerde(type_name)
    elif type_name == 'uint16':
        return IntSerde(type_name)
    elif type_name == 'int32':
        return IntSerde(type_name)
    elif type_name == 'uint32':
        return IntSerde(type_name)
    elif type_name == 'int64':
        return IntSerde(type_name)
    elif type_name == 'uint64':
        return IntSerde(type_name)
    elif type_name == 'str':
        return StringSerde()
    elif type_name == 'bool':
        return BoolSerde()
    elif type_name == 'float32':
        return FloatSerde(type_name)
    elif type_name == 'float64':
        return FloatSerde(type_name)
    if type_name == '[]int':
        return IntListSerde(type_name)
    elif type_name == '[]uint':
        return IntListSerde(type_name)
    elif type_name == '[]int8':
        return IntListSerde(type_name)
    elif type_name == '[]uint8':
        return IntListSerde(type_name)
    elif type_name == '[]int16':
        return IntListSerde(type_name)
    elif type_name == '[]uint16':
        return IntListSerde(type_name)
    elif type_name == '[]int32':
        return IntListSerde(type_name)
    elif type_name == '[]uint32':
        return IntListSerde(type_name)
    elif type_name == '[]int64':
        return IntListSerde(type_name)
    elif type_name == '[]uint64':
        return IntListSerde(type_name)
    elif type_name == '[]str':
        return StringListSerde()
    elif type_name == '[]bool':
        return BoolListSerde()
    elif type_name == '[]float32':
        return FloatListSerde(type_name)
    elif type_name == '[]float64':
        return FloatListSerde(type_name)
    elif type_name == 'bytes':
        return BytesSerde()

    raise ValueError(f"make_default_serde unsupported type: {type_name}")

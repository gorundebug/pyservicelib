#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import struct
from abc import ABC, abstractmethod
from typing import Any, cast, Hashable, Optional, Union
import sys

from pyservicelib.runtime.datastruct import KeyValue


BytesBuffer = Union[bytes, bytearray, memoryview]


UINT_SIZE = 4 if sys.maxsize <= 2**32 else 8
MAX_SIZE_LENGTH = UINT_SIZE
SIZE_PACK_FORMAT = '<I' if UINT_SIZE == 4 else '<Q'


def set_size(b: Union[bytearray, memoryview], offset: int, size: int) -> int:
    struct.pack_into(SIZE_PACK_FORMAT, b, offset, size)
    return UINT_SIZE


def get_size(b: Union[bytearray, memoryview], offset: int) -> tuple[int, int]:
    if len(b) < UINT_SIZE:
        raise ValueError("get_size: size length error")
    return struct.unpack_from(SIZE_PACK_FORMAT, b, offset)[0], UINT_SIZE


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

    @property
    @abstractmethod
    def value_serializer(self) -> Serializer:
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

    @property
    def value_serializer(self) -> Serializer:
        return self._serde


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


class IntListSerde(Serde[list[int]]):
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
        return self.serialize(cast(list[int], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: list[int], b: BytesBuffer) -> bytearray:
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

    def deserialize(self, data: BytesBuffer) -> list[int]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("IntListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}{self._fmt}', data, n))


class FloatListSerde(Serde[list[float]]):
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
        return self.serialize(cast(list[float], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: list[float], b: BytesBuffer) -> bytearray:
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

    def deserialize(self, data: BytesBuffer) -> list[float]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("FloatListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}{self._fmt}', data, n))


class BoolListSerde(Serde[list[bool]]):

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"BoolListSerde: obj is not list")
        return self.serialize(cast(list[bool], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: list[bool], b: BytesBuffer) -> bytearray:
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

    def deserialize(self, data: BytesBuffer) -> list[bool]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count + n:
            raise ValueError("BoolListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'<{count}?', data, n))


class StringListSerde(Serde[list[str]]):

    def __init__(self):
        self._value_serde = StringSerde()

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"StringListSerde: obj is not list")
        return self.serialize(cast(list[str], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize_obj(data)

    def serialize(self, obj: list[str], b: BytesBuffer) -> bytearray:
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

    def deserialize(self, data: BytesBuffer) -> list[str]:
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


class ListSerde(Serde[list[Any]]):
    _value_serde: Serializer

    def __init__(self, value_serde: Serializer):
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError(f"value is not of type list")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        list_value: list[Any] = cast(list[Any], obj)

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

        result: list[Optional[Any]] = [None] * count

        for i in range(count):
            element_length, n = get_size(data, 0)
            data = data[n:]

            if len(data) < element_length:
                raise ValueError("DeserializeObj ListSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(data[:element_length])
            result[i] = element

            data = data[element_length:]

        return result

    def serialize(self, obj: list[Any], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> list[Any]:
        return cast(list[Any], self.deserialize_obj(data))

    @property
    def is_stub(self) -> bool:
        return self._value_serde.is_stub


class TupleSerde(Serde[tuple[Any, ...]]):
    _value_serde: Serializer

    def __init__(self, value_serde: Serializer):
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, tuple):
            raise ValueError(f"value is not of type tuple")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        tuple_value: tuple[Any, ...] = cast(tuple[Any, ...], obj)

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

        result: list[Optional[Any]] = [None] * count

        for i in range(count):
            element_length, n = get_size(data, 0)
            data = data[n:]

            if len(data) < element_length:
                raise ValueError("DeserializeObj ListSerde error (invalid element data)")

            element = self._value_serde.deserialize_obj(data[:element_length])
            result[i] = element

            data = data[element_length:]

        return tuple(result)

    def serialize(self, obj: tuple[Any, ...], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> tuple[Any, ...]:
        return cast(tuple[Any, ...], self.deserialize_obj(data))

    @property
    def is_stub(self) -> bool:
        return self._value_serde.is_stub


class DictSerde(Serde[dict[Any, Any]]):
    _keys_serde: Serializer
    _values_serde: Serializer

    def __init__(self, keys_serde: Serializer, values_serde: Serializer):
        self._keys_serde = keys_serde
        self._values_serde = values_serde

    def serialize(self, obj: dict[Any, Any], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> dict[Any, Any]:
        return cast(dict[Any, Any], self.deserialize_obj(data))

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, dict):
            raise ValueError(f"value is not of type dict")

        if not isinstance(b, bytearray):
            b = bytearray(b)

        dict_value: dict[Any, Any] = cast(dict[Any, Any], obj)

        keys: list[Any] = list(dict_value.keys())
        values: list[Any] = list(dict_value.values())

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

    @property
    def is_stub(self) -> bool:
        return self._values_serde.is_stub or self._keys_serde.is_stub


def make_default_serde(serde_type: str) -> Optional[Serializer]:
    if serde_type == 'int':
        return IntSerde(serde_type)
    elif serde_type == 'uint':
        return IntSerde(serde_type)
    elif serde_type == 'int8':
        return IntSerde(serde_type)
    elif serde_type == 'uint8':
        return IntSerde(serde_type)
    elif serde_type == 'int16':
        return IntSerde(serde_type)
    elif serde_type == 'uint16':
        return IntSerde(serde_type)
    elif serde_type == 'int32':
        return IntSerde(serde_type)
    elif serde_type == 'uint32':
        return IntSerde(serde_type)
    elif serde_type == 'int64':
        return IntSerde(serde_type)
    elif serde_type == 'uint64':
        return IntSerde(serde_type)
    elif serde_type == 'str':
        return StringSerde()
    elif serde_type == 'bool':
        return BoolSerde()
    elif serde_type == 'float32':
        return FloatSerde(serde_type)
    elif serde_type == 'float64':
        return FloatSerde(serde_type)
    if serde_type == '[]int':
        return IntListSerde(serde_type)
    elif serde_type == '[]uint':
        return IntListSerde(serde_type)
    elif serde_type == '[]int8':
        return IntListSerde(serde_type)
    elif serde_type == '[]uint8':
        return IntListSerde(serde_type)
    elif serde_type == '[]int16':
        return IntListSerde(serde_type)
    elif serde_type == '[]uint16':
        return IntListSerde(serde_type)
    elif serde_type == '[]int32':
        return IntListSerde(serde_type)
    elif serde_type == '[]uint32':
        return IntListSerde(serde_type)
    elif serde_type == '[]int64':
        return IntListSerde(serde_type)
    elif serde_type == '[]uint64':
        return IntListSerde(serde_type)
    elif serde_type == '[]str':
        return StringListSerde()
    elif serde_type == '[]bool':
        return BoolListSerde()
    elif serde_type == '[]float32':
        return FloatListSerde(serde_type)
    elif serde_type == '[]float64':
        return FloatListSerde(serde_type)
    elif serde_type == 'bytes':
        return BytesSerde()

    return None

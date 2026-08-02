#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import json
import struct
from abc import ABC, abstractmethod
from dataclasses import asdict, fields as dc_fields, is_dataclass
from datetime import datetime
from types import UnionType
from typing import Any, List, cast, Hashable, Optional, Union, get_args, get_origin, get_type_hints

from ..datastruct import KeyValue


BytesBuffer = Union[bytes, bytearray, memoryview]


SIZE_BYTES = 8
MAX_SIZE_LENGTH = SIZE_BYTES
_SIZE_FMT = ">Q"


def set_size(b: Union[bytearray, memoryview], offset: int, size: int) -> int:
    struct.pack_into(_SIZE_FMT, b, offset, size)
    return SIZE_BYTES


def get_size(b: Union[bytearray, memoryview], offset: int) -> tuple[int, int]:
    if len(b) - offset < SIZE_BYTES:
        raise ValueError("get_size: not enough data")
    return struct.unpack_from(_SIZE_FMT, b, offset)[0], SIZE_BYTES


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

    @property
    @abstractmethod
    def type_name(self) -> str:
        pass


class StreamSerializer(ABC):

    @property
    @abstractmethod
    def is_key_value(self) -> bool:
        pass


class Serde[T](Serializer):

    _type_name: str

    def __init__(self, type_name: str):
        self._type_name = type_name

    @abstractmethod
    def serialize(self, obj: T, b: BytesBuffer) -> bytearray:
        pass

    @abstractmethod
    def deserialize(self, data: BytesBuffer) -> T:
        pass

    @property
    def type_name(self) -> str:
        return self._type_name


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


class StubSerde(Serde[Any]):
    def __init__(self, type_name: str):
        super().__init__(type_name)

    @property
    def is_stub(self) -> bool:
        return True

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        return self.serialize(obj, b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: Any, b: BytesBuffer) -> bytearray:
        raise NotImplementedError(f"serde for type {self._type_name!r} is not implemented")

    def deserialize(self, data: BytesBuffer) -> Any:
        raise NotImplementedError(f"serde for type {self._type_name!r} is not implemented")


class BytesSerde(Serde[bytes]):
    def __init__(self, type_name: str):
        super().__init__(type_name)

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
        return bytes(data[:length])


# IntSerde covers all fixed-size integer types using big-endian encoding.
# Signed types (except int8) use XOR with the sign bit to match Go's sort-order encoding:
#   Go: binary.BigEndian.PutUint16(b, uint16(v)^0x8000)
# int8 is stored as a raw signed byte (no XOR), matching Go's Int8Serde.
class IntSerde(Serde[int]):
    _unsigned_fmt: str  # big-endian unsigned struct format (used with XOR)
    _signed_fmt: str    # big-endian signed struct format (used without XOR)
    _size: int
    _xor_mask: int      # 0 = no XOR (int8, unsigned types)

    def __init__(self, type_name: str):
        super().__init__(type_name)
        if type_name == 'int8':
            self._unsigned_fmt = '>b'   # no XOR: use signed format directly
            self._signed_fmt = '>b'
            self._size = 1
            self._xor_mask = 0
        elif type_name == 'uint8':
            self._unsigned_fmt = '>B'
            self._signed_fmt = '>B'
            self._size = 1
            self._xor_mask = 0
        elif type_name == 'int16':
            self._unsigned_fmt = '>H'
            self._signed_fmt = '>h'
            self._size = 2
            self._xor_mask = 0x8000
        elif type_name == 'uint16':
            self._unsigned_fmt = '>H'
            self._signed_fmt = '>H'
            self._size = 2
            self._xor_mask = 0
        elif type_name == 'int32':
            self._unsigned_fmt = '>I'
            self._signed_fmt = '>i'
            self._size = 4
            self._xor_mask = 0x80000000
        elif type_name == 'uint32':
            self._unsigned_fmt = '>I'
            self._signed_fmt = '>I'
            self._size = 4
            self._xor_mask = 0
        elif type_name == 'int64':
            self._unsigned_fmt = '>Q'
            self._signed_fmt = '>q'
            self._size = 8
            self._xor_mask = 0x8000000000000000
        elif type_name == 'uint64':
            self._unsigned_fmt = '>Q'
            self._signed_fmt = '>Q'
            self._size = 8
            self._xor_mask = 0
        elif type_name == 'int':
            self._unsigned_fmt = '>Q'
            self._signed_fmt = '>q'
            self._size = 8
            self._xor_mask = 0x8000000000000000
        elif type_name == 'uint':
            self._unsigned_fmt = '>Q'
            self._signed_fmt = '>Q'
            self._size = 8
            self._xor_mask = 0
        else:
            raise ValueError(f"IntSerde: invalid type {type_name!r}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, int):
            raise ValueError(f"IntSerde: obj is not int")
        return self.serialize(cast(int, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: int, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        if self._xor_mask:
            bits = self._size * 8
            b.extend(struct.pack(self._unsigned_fmt, (obj & ((1 << bits) - 1)) ^ self._xor_mask))
        else:
            b.extend(struct.pack(self._unsigned_fmt, obj))
        return b

    def deserialize(self, data: BytesBuffer) -> int:
        if len(data) < self._size:
            raise ValueError(f"IntSerde deserialization error: not enough data")
        if self._xor_mask:
            raw = struct.unpack_from(self._unsigned_fmt, data)[0] ^ self._xor_mask
            bits = self._size * 8
            if raw >= (1 << (bits - 1)):
                raw -= (1 << bits)
            return raw
        return struct.unpack_from(self._signed_fmt, data)[0]


# RuneSerde mirrors Go's RuneSerde: BigEndian uint32 with NO XOR, deserialized as int32.
# (Different from Int32Serde which uses XOR 0x80000000.)
class RuneSerde(Serde[int]):
    def __init__(self):
        super().__init__('rune')

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, int):
            raise ValueError("RuneSerde: obj is not int")
        return self.serialize(cast(int, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: int, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        b.extend(struct.pack('>I', obj & 0xFFFFFFFF))
        return b

    def deserialize(self, data: BytesBuffer) -> int:
        if len(data) < 4:
            raise ValueError("RuneSerde deserialization error")
        raw = struct.unpack_from('>I', data)[0]
        return raw if raw < 0x80000000 else raw - 0x100000000


class FloatSerde(Serde[float]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        super().__init__(type_name)
        if type_name == 'float32':
            self._fmt = '>f'
            self._size = 4
        elif type_name == 'float64':
            self._fmt = '>d'
            self._size = 8
        else:
            raise ValueError(f"FloatSerde: invalid type {type_name!r}")

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
        return struct.unpack_from(self._fmt, data)[0]


class BoolSerde(Serde[bool]):
    def __init__(self, type_name: str):
        super().__init__(type_name)

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
    def __init__(self, type_name: str):
        super().__init__(type_name)

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


# IntListSerde serializes fixed-size integer lists with big-endian encoding + XOR for signed types,
# matching Go's Int16ArraySerde, Int32ArraySerde, Int64ArraySerde, IntArraySerde, UIntArraySerde, etc.
class IntListSerde(Serde[list[int]]):
    _unsigned_fmt: str  # element format (unsigned big-endian, used with XOR)
    _signed_fmt: str    # element format (signed big-endian, used without XOR)
    _size: int
    _xor_mask: int

    def __init__(self, type_name: str):
        super().__init__(type_name)
        if type_name == '[]int8':
            self._unsigned_fmt = 'b'
            self._signed_fmt = 'b'
            self._size = 1
            self._xor_mask = 0
        elif type_name == '[]uint8':
            self._unsigned_fmt = 'B'
            self._signed_fmt = 'B'
            self._size = 1
            self._xor_mask = 0
        elif type_name == '[]int16':
            self._unsigned_fmt = 'H'
            self._signed_fmt = 'h'
            self._size = 2
            self._xor_mask = 0x8000
        elif type_name == '[]uint16':
            self._unsigned_fmt = 'H'
            self._signed_fmt = 'H'
            self._size = 2
            self._xor_mask = 0
        elif type_name == '[]int32':
            self._unsigned_fmt = 'I'
            self._signed_fmt = 'i'
            self._size = 4
            self._xor_mask = 0x80000000
        elif type_name == '[]uint32':
            self._unsigned_fmt = 'I'
            self._signed_fmt = 'I'
            self._size = 4
            self._xor_mask = 0
        elif type_name == '[]int64':
            self._unsigned_fmt = 'Q'
            self._signed_fmt = 'q'
            self._size = 8
            self._xor_mask = 0x8000000000000000
        elif type_name == '[]uint64':
            self._unsigned_fmt = 'Q'
            self._signed_fmt = 'Q'
            self._size = 8
            self._xor_mask = 0
        elif type_name == '[]int':
            self._unsigned_fmt = 'Q'
            self._signed_fmt = 'q'
            self._size = 8
            self._xor_mask = 0x8000000000000000
        elif type_name == '[]uint':
            self._unsigned_fmt = 'Q'
            self._signed_fmt = 'Q'
            self._size = 8
            self._xor_mask = 0
        else:
            raise ValueError(f"IntListSerde: invalid type {type_name!r}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError("IntListSerde: obj is not list")
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

        if self._xor_mask:
            bits = self._size * 8
            mask = (1 << bits) - 1
            b.extend(struct.pack(f'>{count}{self._unsigned_fmt}',
                                 *((v & mask) ^ self._xor_mask for v in obj)))
        else:
            b.extend(struct.pack(f'>{count}{self._unsigned_fmt}', *obj))
        return b

    def deserialize(self, data: BytesBuffer) -> list[int]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("IntListSerde: deserialization error: not enough data")

        if self._xor_mask:
            raw = struct.unpack_from(f'>{count}{self._unsigned_fmt}', data, n)
            bits = self._size * 8
            half = 1 << (bits - 1)
            full = 1 << bits
            result = [v ^ self._xor_mask for v in raw]
            return [v if v < half else v - full for v in result]
        return list(struct.unpack_from(f'>{count}{self._signed_fmt}', data, n))


class FloatListSerde(Serde[list[float]]):
    _fmt: str
    _size: int

    def __init__(self, type_name: str):
        super().__init__(type_name)
        if type_name == '[]float32':
            self._fmt = 'f'
            self._size = 4
        elif type_name == '[]float64':
            self._fmt = 'd'
            self._size = 8
        else:
            raise ValueError(f"FloatListSerde: invalid type {type_name!r}")

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError("FloatListSerde: obj is not list")
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
        b.extend(struct.pack(f'>{count}{self._fmt}', *obj))
        return b

    def deserialize(self, data: BytesBuffer) -> list[float]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count * self._size + n:
            raise ValueError("FloatListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'>{count}{self._fmt}', data, n))


class BoolListSerde(Serde[list[bool]]):

    def __init__(self, type_name: str):
        super().__init__(type_name)

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError("BoolListSerde: obj is not list")
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
        struct.pack_into(f'>{count}?', b, n, *obj)
        return b

    def deserialize(self, data: BytesBuffer) -> list[bool]:
        if not isinstance(data, memoryview):
            data = memoryview(data)

        count, n = get_size(data, 0)
        if len(data) < count + n:
            raise ValueError("BoolListSerde: deserialization error: not enough data")
        return list(struct.unpack_from(f'>{count}?', data, n))


class StringListSerde(Serde[list[str]]):

    def __init__(self, type_name: str):
        super().__init__(type_name)

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError("StringListSerde: obj is not list")
        return self.serialize(cast(list[str], obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

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

    def __init__(self, type_name: str, value_serde: Serializer):
        super().__init__(type_name)
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, list):
            raise ValueError("value is not of type list")

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
                raise ValueError("ListSerde deserialize error (invalid element data)")

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

    def __init__(self, type_name: str, value_serde: Serializer):
        super().__init__(type_name)
        self._value_serde = value_serde

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, tuple):
            raise ValueError("value is not of type tuple")

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
                raise ValueError("TupleSerde deserialize error (invalid element data)")

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

    def __init__(self, type_name: str, keys_serde: Serializer, values_serde: Serializer):
        super().__init__(type_name)
        self._keys_serde = keys_serde
        self._values_serde = values_serde

    def serialize(self, obj: dict[Any, Any], b: BytesBuffer) -> bytearray:
        return self.serialize_obj(obj, b)

    def deserialize(self, data: BytesBuffer) -> dict[Any, Any]:
        return cast(dict[Any, Any], self.deserialize_obj(data))

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        if not isinstance(obj, dict):
            raise ValueError("value is not of type dict")

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


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"DataclassJsonSerde: cannot JSON-encode {type(value).__name__}")


def _from_json_value(field_type: Any, value: Any) -> Any:
    if value is None:
        return None
    origin = get_origin(field_type)
    if origin is Union or origin is UnionType:
        args = [a for a in get_args(field_type) if a is not type(None)]
        return _from_json_value(args[0], value) if args else value
    if origin in (list, List):
        (elem_type,) = get_args(field_type) or (Any,)
        return [_from_json_value(elem_type, item) for item in value]
    if is_dataclass(field_type):
        hints = get_type_hints(field_type)
        kwargs = {}
        for f in dc_fields(field_type):
            if f.name in value:
                kwargs[f.name] = _from_json_value(hints[f.name], value[f.name])
        return field_type(**kwargs)
    if field_type is datetime:
        return datetime.fromisoformat(value)
    return value


class DataclassJsonSerde[T](Serde[T]):
    """Generic JSON serde for any @dataclass, using dataclasses.asdict() for
    serialization and dataclasses.fields()/type hints to reconstruct nested
    dataclasses, lists, Optional and datetime fields on deserialization. This
    mirrors what a userver's ADL-based Serialize/Parse or Rust's
    serde_json-backed JsonSerde<T> already gets "for free" from the type
    system -- Python dataclasses have no built-in JSON codec, so this fills
    that gap generically instead of requiring per-type generated code."""

    _cls: type

    def __init__(self, type_name: str, cls: type):
        super().__init__(type_name)
        self._cls = cls

    def serialize_obj(self, obj: Any, b: BytesBuffer) -> bytearray:
        return self.serialize(cast(T, obj), b)

    def deserialize_obj(self, data: BytesBuffer) -> Any:
        return self.deserialize(data)

    def serialize(self, obj: T, b: BytesBuffer) -> bytearray:
        if not isinstance(b, bytearray):
            b = bytearray(b)
        encoded = json.dumps(asdict(obj), default=_json_default).encode('utf-8')
        b.extend(encoded)
        return b

    def deserialize(self, data: BytesBuffer) -> T:
        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytearray):
            data = bytes(data)
        value = json.loads(data)
        return cast(T, _from_json_value(self._cls, value))


def make_default_serde(serde_type: str) -> Optional[Serializer]:
    if serde_type in ('int', 'uint', 'int8', 'uint8', 'int16', 'uint16',
                      'int32', 'uint32', 'int64', 'uint64'):
        return IntSerde(serde_type)
    if serde_type in ('float32', 'float64'):
        return FloatSerde(serde_type)
    if serde_type == 'bool':
        return BoolSerde(serde_type)
    if serde_type == 'str':
        return StringSerde(serde_type)
    if serde_type == 'rune':
        return RuneSerde()
    if serde_type == 'bytes':
        return BytesSerde(serde_type)
    if serde_type in ('[]int', '[]uint', '[]int8', '[]uint8', '[]int16', '[]uint16',
                      '[]int32', '[]uint32', '[]int64', '[]uint64'):
        return IntListSerde(serde_type)
    if serde_type in ('[]float32', '[]float64'):
        return FloatListSerde(serde_type)
    if serde_type == '[]bool':
        return BoolListSerde(serde_type)
    if serde_type == '[]str':
        return StringListSerde(serde_type)
    return None

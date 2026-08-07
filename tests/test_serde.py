#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import sys
from typing import cast
import os
from pathlib import Path
import pytest

from pyservicelib_gorundebug.runtime.serde import Serde
from pyservicelib_gorundebug.runtime.serviceapp import  ServiceAppLoader
from pyservicelib_gorundebug.runtime.context import default_context
from pyservicelib_gorundebug.runtime.config import  ConfigSettings
from pyservicelib_gorundebug.runtime.serde import (
    IntListSerde, IntSerde, StringSerde, BoolListSerde, StringListSerde,
    FloatSerde, RuneSerde, StubSerde, DataclassJsonSerde,
)

from .mockservice import MockService, MockServiceConfig, MockServiceDependency

@pytest.mark.asyncio
async def test_serde_type_dict():
    config_dir = str(Path(__file__).parent / "mockservice" / "config")
    sys.argv = [sys.argv[0],
                "--config", f"{config_dir}/config.yaml"]
    value = {1: True, 2: False, 3: True}

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("IncomeService", MockServiceDependency(), ConfigSettings())
    ctx = default_context()
    ser = cast(Serde[dict[int, bool]], service.get_type_serde("MapType"))

    b = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(b)

    await service.stop(ctx)
    await service.release()
    assert value == value_copy


def test_serde_ints():
    values = [1, 2, 3]
    ser = IntListSerde("[]int")
    data = ser.serialize(values, bytearray())
    values_copy = ser.deserialize(data)
    assert values == values_copy


def test_serde_bools():
    values = [True, False, True]
    ser = BoolListSerde("[]bool")
    data = ser.serialize(values, bytearray())
    values_copy = ser.deserialize(data)
    assert values == values_copy


def test_serde_strings():
    values = ["test1", "", "test3"]
    ser = StringListSerde("[]str")
    data = ser.serialize(values, bytearray())
    values_copy = ser.deserialize(data)
    assert values == values_copy


def test_serde_int():
    value = 100
    ser = IntSerde("int")
    data = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(data)
    assert value == value_copy


def test_serde_string():
    value = "test"
    ser = StringSerde("str")
    data = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(data)
    assert value == value_copy


# --- Byte-level tests matching Go's binary encoding ---

# Go Int8Serde: append(b, byte(value)) — no XOR, raw byte
def test_int8_negative_raw_byte():
    ser = IntSerde('int8')
    assert ser.serialize(-1, bytearray()) == bytearray(b'\xff')
    assert ser.serialize(-128, bytearray()) == bytearray(b'\x80')
    assert ser.serialize(127, bytearray()) == bytearray(b'\x7f')
    assert ser.deserialize(b'\xff') == -1
    assert ser.deserialize(b'\x80') == -128


# Go Int16Serde: BigEndian uint16(v)^0x8000
def test_int16_xor_big_endian():
    ser = IntSerde('int16')
    # 0 → 0x8000 big-endian = [0x80, 0x00]
    assert ser.serialize(0, bytearray()) == bytearray(b'\x80\x00')
    # -1 → 0xFFFF^0x8000 = 0x7FFF → [0x7F, 0xFF]
    assert ser.serialize(-1, bytearray()) == bytearray(b'\x7f\xff')
    # -32768 → 0x0000 → [0x00, 0x00]
    assert ser.serialize(-32768, bytearray()) == bytearray(b'\x00\x00')
    # 32767 → 0xFFFF → [0xFF, 0xFF]
    assert ser.serialize(32767, bytearray()) == bytearray(b'\xff\xff')
    assert ser.deserialize(b'\x7f\xff') == -1
    assert ser.deserialize(b'\x80\x00') == 0


# Go UInt16Serde: BigEndian PutUint16 — no XOR
def test_uint16_big_endian_no_xor():
    ser = IntSerde('uint16')
    assert ser.serialize(1, bytearray()) == bytearray(b'\x00\x01')
    assert ser.serialize(0xFFFF, bytearray()) == bytearray(b'\xff\xff')
    assert ser.deserialize(b'\x00\x01') == 1


# Go Int32Serde: BigEndian uint32(v)^0x80000000
def test_int32_xor_big_endian():
    ser = IntSerde('int32')
    assert ser.serialize(0, bytearray()) == bytearray(b'\x80\x00\x00\x00')
    assert ser.serialize(-1, bytearray()) == bytearray(b'\x7f\xff\xff\xff')
    assert ser.deserialize(b'\x7f\xff\xff\xff') == -1


# Go Int64Serde: BigEndian uint64(v)^0x8000000000000000
def test_int64_xor_big_endian():
    ser = IntSerde('int64')
    assert ser.serialize(0, bytearray()) == bytearray(b'\x80\x00\x00\x00\x00\x00\x00\x00')
    assert ser.serialize(-1, bytearray()) == bytearray(b'\x7f\xff\xff\xff\xff\xff\xff\xff')
    assert ser.deserialize(b'\x7f\xff\xff\xff\xff\xff\xff\xff') == -1


# Go RuneSerde: BigEndian PutUint32 — NO XOR (unlike Int32Serde)
def test_rune_no_xor_big_endian():
    ser = RuneSerde()
    # 'A' = 65
    assert ser.serialize(65, bytearray()) == bytearray(b'\x00\x00\x00\x41')
    # -1 as int32 → uint32 = 0xFFFFFFFF → [0xFF, 0xFF, 0xFF, 0xFF]
    assert ser.serialize(-1, bytearray()) == bytearray(b'\xff\xff\xff\xff')
    assert ser.deserialize(b'\xff\xff\xff\xff') == -1
    assert ser.deserialize(b'\x00\x00\x00\x41') == 65


# Go Float32Serde: math.Float32bits → BigEndian (1.0 = 0x3F800000)
def test_float32_big_endian():
    ser = FloatSerde('float32')
    assert ser.serialize(1.0, bytearray()) == bytearray(b'\x3f\x80\x00\x00')
    assert abs(ser.deserialize(b'\x3f\x80\x00\x00') - 1.0) < 1e-6


# Go Float64Serde: math.Float64bits → BigEndian (1.0 = 0x3FF0000000000000)
def test_float64_big_endian():
    ser = FloatSerde('float64')
    assert ser.serialize(1.0, bytearray()) == bytearray(b'\x3f\xf0\x00\x00\x00\x00\x00\x00')
    assert ser.deserialize(b'\x3f\xf0\x00\x00\x00\x00\x00\x00') == 1.0


# Go Int16ArraySerde: BigEndian uint16(v)^0x8000 for each element
def test_int16_list_xor_big_endian():
    ser = IntListSerde('[]int16')
    data = ser.serialize([-1, 0, 32767], bytearray())
    assert ser.deserialize(data) == [-1, 0, 32767]
    # -1 → 0x7FFF, 0 → 0x8000, 32767 → 0xFFFF
    # length header (big-endian 3) + 3 elements × 2 bytes
    import struct, sys
    size_fmt = '>I' if sys.maxsize <= 2**32 else '>Q'
    header_size = 4 if sys.maxsize <= 2**32 else 8
    count = struct.unpack_from(size_fmt, data)[0]
    assert count == 3
    vals = struct.unpack_from('>3H', data, header_size)
    assert vals == (0x7FFF, 0x8000, 0xFFFF)


# Go StringSerde: BigEndian length header
def test_string_big_endian_length():
    import struct, sys
    ser = StringSerde('str')
    data = ser.serialize('AB', bytearray())
    size_fmt = '>I' if sys.maxsize <= 2**32 else '>Q'
    header_size = 4 if sys.maxsize <= 2**32 else 8
    length = struct.unpack_from(size_fmt, data)[0]
    assert length == 2
    assert data[header_size:header_size + 2] == bytearray(b'AB')


# StubSerde raises on serialize/deserialize (mirrors Go's error return)
def test_stub_serde_raises():
    ser = StubSerde('MyType')
    assert ser.is_stub
    with pytest.raises(NotImplementedError):
        ser.serialize(42, bytearray())
    with pytest.raises(NotImplementedError):
        ser.deserialize(b'\x00')


# Roundtrip tests for negative values across all signed types
@pytest.mark.parametrize("type_name,value", [
    ('int8', -1), ('int8', -128), ('int8', 127),
    ('int16', -1), ('int16', -32768), ('int16', 32767),
    ('int32', -1), ('int32', -2147483648), ('int32', 2147483647),
    ('int64', -1), ('int64', -(2**63)), ('int64', 2**63 - 1),
    ('int', -1), ('int', 0), ('int', 100),
    ('uint8', 0), ('uint8', 255),
    ('uint16', 0), ('uint16', 65535),
    ('uint32', 0), ('uint32', 4294967295),
    ('uint64', 0), ('uint64', 2**64 - 1),
    ('uint', 0), ('uint', 1000),
])
def test_int_serde_roundtrip(type_name, value):
    ser = IntSerde(type_name)
    assert ser.deserialize(ser.serialize(value, bytearray())) == value


@pytest.mark.parametrize("serializer", [IntListSerde("[]int")])
def test_benchmark_serde_ints(benchmark, serializer):
    values = list(range(1, 40001))

    def ser_deser():
        data = serializer.serialize(values, bytearray())
        values_copy = serializer.deserialize(data)
        assert values == values_copy

    benchmark(ser_deser)


# --- DataclassJsonSerde: generic JSON round-trip for @dataclass types ---

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(slots=True)
class _Item:
    name: str = ""
    quantity: int = 0


@dataclass(slots=True)
class _Widget:
    id: str
    items: list[_Item] = field(default_factory=list)
    amount: float = 0.0
    created_at: datetime | None = None


def test_dataclass_json_serde_roundtrip():
    ser = DataclassJsonSerde("Widget", _Widget)
    widget = _Widget(
        id="w-1",
        items=[_Item("a", 1), _Item("b", 2)],
        amount=9.5,
        created_at=datetime.now(timezone.utc),
    )
    data = ser.serialize(widget, bytearray())
    assert ser.deserialize(data) == widget


def test_dataclass_json_serde_preserves_prefix_buffer():
    ser = DataclassJsonSerde("Widget", _Widget)
    widget = _Widget(id="w-2")
    buf = bytearray(b"\xab")
    out = ser.serialize(widget, buf)
    assert out[0] == 0xab
    assert ser.deserialize(bytes(out[1:])) == widget


def test_dataclass_json_serde_optional_none():
    ser = DataclassJsonSerde("Widget", _Widget)
    widget = _Widget(id="w-3", created_at=None)
    data = ser.serialize(widget, bytearray())
    back = ser.deserialize(data)
    assert back == widget
    assert back.created_at is None
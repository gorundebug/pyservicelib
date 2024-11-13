#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import cast
import os
from pathlib import Path
import pytest

from ..pyservicelib.runtime.serde import Serde
from ..pyservicelib.runtime.serviceapp import  ServiceAppLoader
from ..pyservicelib.runtime.context import default_context
from ..pyservicelib.runtime.config import  ConfigSettings
from .mockservice import MockService, MockServiceConfig, MockServiceDependency
from ..pyservicelib.runtime.serde import IntListSerde, IntSerde, StringSerde, BoolListSerde, StringListSerde

@pytest.mark.asyncio
async def test_serde_type_dict():
    os.chdir(Path(__file__).parent)
    value = {1: True, 2: False, 3: True}

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("MockService", MockServiceDependency(), ConfigSettings())
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


@pytest.mark.parametrize("serializer", [IntListSerde("[]int")])
def test_benchmark_serde_ints(benchmark, serializer):
    values = list(range(1, 40001))

    def ser_deser():
        data = serializer.serialize(values, bytearray())
        values_copy = serializer.deserialize(data)
        assert values == values_copy

    benchmark(ser_deser)
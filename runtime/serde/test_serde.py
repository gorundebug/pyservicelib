#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import pytest

from pyservicelib.runtime.serde import IntsSerde, IntsSerde2, IntsSerde3, IntSerde, StringSerde


def test_serde_ints():
    values = [1, 2, 3]
    ser = IntsSerde2()
    data = ser.serialize(values, bytearray())
    values_copy = ser.deserialize(data)
    assert values == values_copy


def test_serde_int():
    value = 100
    ser = IntSerde()
    data = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(data)
    assert value == value_copy


def test_serde_string():
    value = "test"
    ser = StringSerde()
    data = ser.serialize(value, bytearray())
    value_copy = ser.deserialize(data)
    assert value == value_copy


@pytest.mark.parametrize("serializer", [IntsSerde(), IntsSerde2(), IntsSerde3()])
def test_benchmark_serde_ints(benchmark, serializer):
    values = list(range(1, 40001))

    def ser_deser():
        data = serializer.serialize(values, bytearray())
        values_copy = serializer.deserialize(data)
        assert values == values_copy

    benchmark(ser_deser)
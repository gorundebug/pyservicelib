#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import os
from pathlib import Path
from typing import get_origin, Any, Optional
from collections.abc import Iterable
import pytest

from pyservicelib_gorundebug.runtime.config import ConfigSettings
from pyservicelib_gorundebug.runtime.serviceapp import ServiceAppLoader
from pyservicelib_gorundebug import transformation

from .mockservice import MockService, MockServiceConfig, MockServiceDependency

class Value[T]:
    value: Optional[T]

    def __init__(self):
        self.value = None

    def set_value(self, value: T):
        self.value = value


class ClassA[T: Iterable, R]:
    def __init__(self):
        pass

    def check_a(self, value: Any) -> bool:
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type

        if not issubclass(orig_type, Iterable):
            return False

        genetic_type = self.__orig_class__.__args__[1] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type
        if not isinstance(value, orig_type):
            return False

        test_value = Value[R]()
        test_value.set_value(value)
        return test_value.value == value


class ClassB[T: Iterable, R]:
    def __init__(self):
        pass

    def check_b(self, value: Any) -> bool:
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type

        if not issubclass(orig_type, Iterable):
            return False

        genetic_type = self.__orig_class__.__args__[1] #type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type
        if not isinstance(value, orig_type):
            return False

        test_value = Value[R]()
        test_value.set_value(value)
        return test_value.value == value


class ClassC[T: Iterable, R](ClassA[T, R], ClassB[T, R]):

    def __init__(self):
        super().__init__()


class ClassD[T: Iterable, R](ClassC[T, R]):

    def __init__(self):
        super().__init__()


def test_type_check():

    stream = ClassD[list[int], int]()
    assert stream.check_a(5) == True
    assert stream.check_b(5) == True

    stream1 = ClassD[tuple, float]()
    assert stream1.check_a(5.0) == True
    assert stream1.check_b(5.0) == True

    stream2 = ClassC[int, float]() #type: ignore[type-var]
    assert stream2.check_a(5.0) == False
    assert stream2.check_b(5.0) == False

    stream3 = ClassC[list[int], float]()
    assert stream3.check_a(5) == False
    assert stream3.check_b(5) == False

    stream4 = ClassC[dict[int, int], float]()
    assert stream4.check_a(5) == False
    assert stream4.check_b(5) == False


@pytest.mark.asyncio
async def test_input_stream():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("MockService", MockServiceDependency(), ConfigSettings())
    stream = transformation.Input[int]("Input", service)
    assert stream.type_name == "int"

@pytest.mark.benchmark(group="slots")
def test_benchmark_with_slots(benchmark):
    class WithSlots:
        __slots__ = ['x', 'y']
        def __init__(self, x, y):
            self.x = x
            self.y = y

    def test():
        objs = [WithSlots(i, i + 1) for i in range(100000)]

    benchmark(test)

@pytest.mark.benchmark(group="slots")
def test_benchmark_without_slots(benchmark):
    class WithoutSlots:
        def __init__(self, x, y):
            self.x = x
            self.y = y

    def test():
        objs = [WithoutSlots(i, i + 1) for i in range(100000)]

    benchmark(test)




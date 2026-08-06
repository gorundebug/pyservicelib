#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import os
import sys
from pathlib import Path
from typing import get_origin, Any, Optional
from types import SimpleNamespace
from collections.abc import Iterable
import pytest

from pyservicelib_gorundebug.runtime.config import ConfigSettings
from pyservicelib_gorundebug.runtime.serviceapp import ServiceAppLoader
from pyservicelib_gorundebug.runtime.common import CallerStatistics, DirectCaller
from pyservicelib_gorundebug import transformation
from pyservicelib_gorundebug.operators.split import SplitLink

from .mockservice import MockService, MockServiceConfig, MockServiceDependency


def test_function_call_async_flag_only_changes_caller_metadata():
    async def consume(_value: int) -> None:
        pass

    consumer = SimpleNamespace(
        stream=SimpleNamespace(name="target"),
        consume=consume,
    )
    source = SimpleNamespace(name="source", consumer=consumer)

    sync_caller = DirectCaller(source, CallerStatistics(), async_=False)
    async_caller = DirectCaller(source, CallerStatistics(), async_=True)

    assert sync_caller.is_async is False
    assert async_caller.is_async is True

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
    config_dir = str(Path(__file__).parent / "mockservice" / "config")
    sys.argv = [sys.argv[0],
                "--config", f"{config_dir}/config.yaml"]

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load("IncomeService", MockServiceDependency(), ConfigSettings())
    cfg = service.config.get_input_stream_config("InputRequest")
    assert cfg is not None
    stream = transformation.Input[int, Any, Any](cfg, service)
    assert stream.type_name == "int"


def test_split_link_type_name_does_not_depend_on_orig_class():
    link = object.__new__(SplitLink)
    link._split_stream = SimpleNamespace(type_name="int")

    assert not hasattr(link, "__orig_class__")
    assert link.type_name == "int"


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

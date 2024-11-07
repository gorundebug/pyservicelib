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

from pyservicelib.runtime.config import ConfigSettings
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.serviceapp import ServiceAppLoader
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency
from pyservicelib import transformation

class Value[T]:
    value: Optional[T]

    def __init__(self):
        self.value = None

    def set_value(self, value: T):
        self.value = value


class Stream[T: Iterable, R]:
    def __init__(self):
        pass

    def check(self, value: Any) -> bool:
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


class DerivedStream[T: Iterable, R](Stream[T, R]):

    def __init__(self):
        super().__init__()


class DerivedFromDerivedStream[T: Iterable, R](DerivedStream[T, R]):

    def __init__(self):
        super().__init__()


def test_type_check():

    stream = DerivedFromDerivedStream[list[int], int]()
    assert stream.check(5) == True

    stream1 = DerivedFromDerivedStream[tuple, float]()
    assert stream1.check(5.0) == True

    stream2 = DerivedStream[int, float]() #type: ignore[type-var]
    assert stream2.check(5.0) == False

    stream3 = DerivedStream[list[int], float]()
    assert stream3.check(5) == False

    stream4 = DerivedStream[dict[int, int], float]()
    assert stream4.check(5) == False


@pytest.mark.asyncio
async def test_input_stream():
    os.chdir(Path(__file__).parent)

    service = await ServiceAppLoader[MockService, MockServiceConfig]().init("MockService", MockServiceDependency(), ConfigSettings())
    stream = transformation.Input[int]("Input", service)
    assert stream.type_name == "int"


def test_benchmark_sync(benchmark):
    counter = 0
    def sync_func():
        nonlocal counter
        counter += 1

    def test():
        sync_func()

    benchmark(test)




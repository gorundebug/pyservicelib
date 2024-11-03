#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import List, get_origin, Any, Dict, Optional
from collections.abc import Iterable


class TestValue[T]:
    value: Optional[T]

    def __init__(self):
        self.value = None

    def set_value(self, value: T):
        self.value = value


class TestStream[T: Iterable, R]:
    def __init__(self):
        pass

    def check(self, value: Any) -> bool:
        genetic_type = self.__orig_class__.__args__[0] #pyright: ignore
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type

        if not issubclass(orig_type, Iterable):
            return False

        genetic_type = self.__orig_class__.__args__[1] #pyright: ignore
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type
        if not isinstance(value, orig_type):
            return False

        test_value = TestValue[R]()
        test_value.set_value(value)
        return test_value.value == value


class DerivedStream[T: Iterable, R](TestStream[T, R]):

    def __init__(self):
        super().__init__()


class DerivedFromDerivedStream[T: Iterable, R](DerivedStream[T, R]):

    def __init__(self):
        super().__init__()


def test_type_check():
    stream = DerivedFromDerivedStream[List[int], int]()
    assert stream.check(5) == True

    stream1 = DerivedFromDerivedStream[tuple, float]()
    assert stream1.check(5.0) == True

    stream2 = DerivedStream[int, float]() #pyright: ignore
    assert stream2.check(5.0) == False

    stream3 = DerivedStream[List[int], float]()
    assert stream3.check(5) == False

    stream4 = DerivedStream[Dict[int, int], float]()
    assert stream4.check(5) == False


def test_benchmark_sync(benchmark):
    counter = 0
    def sync_func():
        nonlocal counter
        counter += 1

    def test():
        sync_func()

    benchmark(test)
    print(counter)


async def test_something(benchmark):

    counter = 0
    async def func():
        nonlocal counter
        counter += 1

    async def async_test():
        await func()

    await benchmark(async_test)
    print(counter)





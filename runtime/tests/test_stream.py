#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

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





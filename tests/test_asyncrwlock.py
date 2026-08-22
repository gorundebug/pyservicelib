import asyncio

import pytest

from pyservicelib_gorundebug.runtime.utils.asyncrwlock import AsyncRWLock


@pytest.mark.asyncio
async def test_readers_overlap_and_writer_waits() -> None:
    lock = AsyncRWLock()
    both_reading = asyncio.Event()
    release_readers = asyncio.Event()
    readers = 0

    async def read() -> None:
        nonlocal readers
        async with lock.read_lock():
            readers += 1
            if readers == 2:
                both_reading.set()
            await release_readers.wait()

    first = asyncio.create_task(read())
    second = asyncio.create_task(read())
    await both_reading.wait()

    writer_entered = asyncio.Event()

    async def write() -> None:
        async with lock.write_lock():
            writer_entered.set()

    writer = asyncio.create_task(write())
    await asyncio.sleep(0)
    assert not writer_entered.is_set()
    release_readers.set()
    await asyncio.gather(first, second, writer)
    assert writer_entered.is_set()

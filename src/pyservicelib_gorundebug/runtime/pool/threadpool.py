#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import threading
import asyncio
from asyncio import AbstractEventLoop
from typing import Callable, Any
from queue import Queue


class AsyncThread:
    _loop: asyncio.AbstractEventLoop
    _thread: threading.Thread
    _task_queue: Queue

    def __init__(self, task_queue: Queue):
        self._task_queue = task_queue
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._executor())

    async def _executor(self):
        while True:
            # run_in_executor prevents blocking the event loop while waiting for tasks
            coro, future, args, kwargs = await self._loop.run_in_executor(
                None, self._task_queue.get
            )
            if coro is None:
                self._task_queue.task_done()
                break
            try:
                result = await coro(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                self._task_queue.task_done()

    def join(self):
        self._thread.join()


class AsyncFuture:
    _future: asyncio.Future
    _loop: AbstractEventLoop

    def __init__(self, loop: AbstractEventLoop):
        self._loop = loop
        self._future = loop.create_future()

    async def result(self) -> Any:
        return await self._future

    def set_result(self, result: Any = None) -> None:
        self._loop.call_soon_threadsafe(self._future.set_result, result)

    def set_exception(self, e: BaseException) -> None:
        self._loop.call_soon_threadsafe(self._future.set_exception, e)


class AsyncThreadPoolExecutor:
    _task_queue: Queue

    def __init__(self, max_workers: int):
        self._task_queue = Queue()
        self.threads: list[AsyncThread] = [AsyncThread(self._task_queue) for _ in range(max_workers)]

    def add_task(self, coro: Callable[..., Any], *args, **kwargs) -> AsyncFuture:
        future = AsyncFuture(asyncio.get_running_loop())
        self._task_queue.put((coro, future, args, kwargs))
        return future

    def shutdown(self):
        for _ in self.threads:
            self._task_queue.put((None, None, None, None))
        for thread in self.threads:
            thread.join()

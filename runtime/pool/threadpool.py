#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import threading
import asyncio
from asyncio import AbstractEventLoop
from typing import Callable, Any, Optional
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
            coro, future, args, kwargs = self._task_queue.get()
            if coro is None:
                break
            try:
                result = await coro(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                self._task_queue.task_done()
        self._loop.stop()

    def join(self):
        self._thread.join()


class AsyncFuture(object):
    _event: asyncio.Event
    _result: Any
    _exception: Optional[BaseException]
    _loop: AbstractEventLoop

    def __init__(self, loop: AbstractEventLoop):
        self._event = asyncio.Event()
        self._loop = loop
        self._result = None
        self._exception = None

    async def result(self) -> Any:
        await self._event.wait()
        if self._exception:
            raise self._exception
        return self._result

    async def _done(self):
        self._event.set()

    def set_result(self, result: Any = None):
        self._result = result
        asyncio.run_coroutine_threadsafe(self._done(), self._loop)

    def set_exception(self, e: BaseException):
        self._exception = e
        asyncio.run_coroutine_threadsafe(self._done(), self._loop)


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
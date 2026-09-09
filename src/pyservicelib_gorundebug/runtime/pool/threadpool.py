#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import threading
import asyncio
from asyncio import AbstractEventLoop
from typing import Callable, Any
from queue import Queue, Empty
import os
from contextvars import copy_context


class AsyncThread:
    _loop: asyncio.AbstractEventLoop
    _thread: threading.Thread
    _task_queue: Queue

    def __init__(self, task_queue: Queue):
        self._task_queue = task_queue
        self._loop = asyncio.new_event_loop()
        self._available = asyncio.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._executor())
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.run_until_complete(self._loop.shutdown_default_executor())
            self._loop.close()

    def wake(self):
        try:
            self._loop.call_soon_threadsafe(self._available.set)
        except RuntimeError:
            if not self._loop.is_closed():
                raise

    async def _executor(self):
        while True:
            self._available.clear()
            try:
                entry = self._task_queue.get_nowait()
            except Empty:
                await self._available.wait()
                continue
            coro, future, args, kwargs, context = entry
            if coro is None:
                self._task_queue.task_done()
                break
            try:
                result = await asyncio.create_task(coro(*args, **kwargs), context=context)
                future.set_result(result)
                del result
            except (Exception, asyncio.CancelledError) as e:
                future.set_exception(e)
            finally:
                self._task_queue.task_done()
            del entry, coro, future, args, kwargs, context

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
        def complete():
            if not self._future.done():
                self._future.set_result(result)
        self._loop.call_soon_threadsafe(complete)

    def set_exception(self, e: BaseException) -> None:
        def complete():
            if not self._future.done():
                self._future.set_exception(e)
        self._loop.call_soon_threadsafe(complete)


class AsyncThreadPoolExecutor:
    _task_queue: Queue

    def __init__(self, max_workers: int):
        if max_workers < 0:
            raise ValueError("max_workers must be non-negative")
        max_workers = max_workers or os.cpu_count() or 1
        self._stopped = False
        self._task_queue = Queue()
        self.threads: list[AsyncThread] = [AsyncThread(self._task_queue) for _ in range(max_workers)]

    def add_task(self, coro: Callable[..., Any], *args, **kwargs) -> AsyncFuture:
        if self._stopped:
            raise RuntimeError("thread pool is stopped")
        future = AsyncFuture(asyncio.get_running_loop())
        self._task_queue.put((coro, future, args, kwargs, copy_context()))
        for thread in self.threads:
            thread.wake()
        return future

    def shutdown(self):
        """Blocking close for synchronous callers; async callers should await aclose()."""
        if not self._stopped:
            self._stopped = True
            for _ in self.threads:
                self._task_queue.put((None, None, None, None, None))
            for thread in self.threads:
                thread.wake()
        for thread in self.threads:
            thread.join()

    async def aclose(self):
        """Drain and join native workers without blocking the caller's event loop."""
        # Seal admission on the owning event loop before joining in a thread.
        if not self._stopped:
            self._stopped = True
            for _ in self.threads:
                self._task_queue.put((None, None, None, None, None))
            for thread in self.threads:
                thread.wake()
        await asyncio.to_thread(self.shutdown)

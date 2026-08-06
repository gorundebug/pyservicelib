#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import logging
from logging.handlers import QueueHandler, QueueListener
from logging import StreamHandler
from queue import Queue
from typing import Optional
import asyncio

from ...environment.log import Logger, LogsEngine, Config, Field


def _format_fields(fields: tuple[Field, ...]) -> str:
    if not fields:
        return ""
    return "  " + "  ".join(f"{f.key}={f.string_value()!r}" for f in fields)


class AsyncLogger(Logger):
    _log: logging.Logger
    _que: Queue
    _listener: QueueListener
    _listener_task: asyncio.Task
    _init_lock: asyncio.Lock
    _ready: bool

    def __init__(
        self,
        cfg: Optional[Config] = None,
        name: Optional[str] = None,
        level: int = logging.DEBUG,
    ):
        self._init_lock = asyncio.Lock()
        self._ready = False
        # Never install ServiceLib's queue handler on the root logger. Doing so
        # also enables and forwards verbose records from dependencies such as
        # grpc.aio, turning a single framework logger into per-request logging.
        self._log = logging.getLogger(name or "pyservicelib")
        self._log.propagate = False
        self._que = Queue()
        handler = QueueHandler(self._que)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self._log.addHandler(handler)
        self._log.setLevel(level)
        self._listener = QueueListener(self._que, StreamHandler())

    def debug(self, msg: str, *fields: Field) -> None:
        self._log.debug(msg + _format_fields(fields))

    def info(self, msg: str, *fields: Field) -> None:
        self._log.info(msg + _format_fields(fields))

    def warn(self, msg: str, *fields: Field) -> None:
        self._log.warning(msg + _format_fields(fields))

    def error(self, msg: str, *fields: Field) -> None:
        self._log.error(msg + _format_fields(fields))

    async def _que_listener(self):
        try:
            self._listener.start()
            while True:
                await asyncio.sleep(60)
        finally:
            self._listener.stop()

    async def _start(self):
        self._listener_task = asyncio.create_task(self._que_listener())
        await asyncio.sleep(0)

    async def stop(self):
        if self._ready:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

    async def logger(self) -> "AsyncLogger":
        if not self._ready:
            async with self._init_lock:
                if not self._ready:
                    await self._start()
                    self._ready = True
        return self


class AsyncLogsEngine(LogsEngine):
    _default_logger: AsyncLogger
    _engine: Optional["AsyncLogsEngine"] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(self):
        self._default_logger = AsyncLogger()
        self._ready = False

    def default_logger(self, cfg: Optional[Config] = None) -> Logger:
        return self._default_logger

    async def shutdown(self) -> None:
        await self._default_logger.stop()

    @classmethod
    async def engine(cls) -> "AsyncLogsEngine":
        if cls._engine is None:
            async with cls._lock:
                if cls._engine is None:
                    engine = AsyncLogsEngine()
                    await engine._default_logger.logger()
                    cls._engine = engine
        return cls._engine

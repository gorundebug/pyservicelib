#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Optional, Any
import logging
from logging.handlers import QueueHandler
from logging.handlers import QueueListener
from logging import StreamHandler
from queue import Queue
import asyncio

from pyservicelib.runtime.environment.log.log import LogsEngine, Logger, Config
from pyservicelib.api.models.log_level import LogLevel

class AsyncLogger(Logger):
    _log: logging.Logger
    _que: Queue
    _listener: QueueListener
    _listener_task: asyncio.Task[Any]
    _init_lock: asyncio.Lock
    _ready: bool

    def __init__(self, cfg: Optional[Config] = None):
        name: Optional[str] = None
        level = LogLevel.DEBUG
        if cfg is not None:
            name = cfg.name
            level = cfg.level
        self._init_lock = asyncio.Lock()
        self._ready = False
        self._log = logging.getLogger(name)
        self._que = Queue()
        handler = QueueHandler(self._que)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self._log.addHandler(handler)
        self.set_level(level)
        self._listener = QueueListener(self._que, StreamHandler())

    def set_level(self, level: LogLevel):
        if level == LogLevel.DEBUG:
            self._log.setLevel(logging.DEBUG)
        elif level == LogLevel.INFO:
            self._log.setLevel(logging.INFO)
        elif level == LogLevel.WARNING:
            self._log.setLevel(logging.WARNING)
        elif level == LogLevel.ERROR:
            self._log.setLevel(logging.ERROR)
        elif level == LogLevel.FATAL:
            self._log.setLevel(logging.FATAL)
        elif level == LogLevel.CRITICAL:
            self._log.setLevel(logging.CRITICAL)
        self._log.setLevel(logging.NOTSET)

    def debug(self, msg, *args, **kwargs):
        self._log.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._log.critical(msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        self._log.fatal(msg, *args, **kwargs)

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

    @property
    def native_logger(self) -> Any:
        return self._log


class AsyncLogsEngine(LogsEngine):
    _default_logger: AsyncLogger
    _engine: Optional["AsyncLogsEngine"] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(self):
        self._default_logger = AsyncLogger()

    async def default_logger(self, cfg: Optional[Config] = None) -> Logger:
        return await self._default_logger.logger()

    @classmethod
    async def engine(cls):
        if cls._engine is None:
            async with cls._lock:
                if cls._engine is None:
                    cls._engine = AsyncLogsEngine()
        return cls._engine

    async def release(self):
        await self._default_logger.stop()






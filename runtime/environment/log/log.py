#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
from dataclasses import dataclass
from typing import Optional, Any
from abc import ABC, abstractmethod

from pyservicelib.api.models.log_level import LogLevel

@dataclass
class Config:
    name: str
    level: LogLevel

class Logger(ABC):
    @abstractmethod
    def set_level(self, level: LogLevel):
        pass

    @abstractmethod
    def debug(self, msg, *args, **kwargs):
        pass

    @abstractmethod
    def info(self, msg, *args, **kwargs):
        pass

    @abstractmethod
    def warning(self, msg, *args, **kwargs):
        pass

    def warn(self, msg, *args, **kwargs):
        self.warning(msg, *args, **kwargs)

    @abstractmethod
    def error(self, msg, *args, **kwargs):
       pass

    def exception(self, msg, *args, exc_info=True, **kwargs):
        self.error(msg, *args, exc_info=exc_info, **kwargs)

    @abstractmethod
    def critical(self, msg, *args, **kwargs):
       pass

    @abstractmethod
    def fatal(self, msg, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def native_logger(self) -> Any:
        pass


class LogsEngine(ABC):

    @abstractmethod
    async def default_logger(self, cfg: Optional[Config] = None) -> Logger:
        pass

    @abstractmethod
    async def release(self):
        pass


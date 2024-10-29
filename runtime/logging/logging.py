#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from enum import Enum
from pyservicelib.runtime.logging.asynclog import AsyncLogsEngine
from pyservicelib.runtime.environment.log import LogsEngine

class LogsEngineType(str, Enum):

    ASYNCLOG = 'ASYNCLOG'


class LogsEngineFactory:

    @classmethod
    async def create_logs_engine(cls, engine_type: LogsEngineType) -> LogsEngine:
        if engine_type == LogsEngineType.ASYNCLOG:
            return await AsyncLogsEngine.engine()
        raise ValueError(f"Unsupported logs engine type {engine_type}")
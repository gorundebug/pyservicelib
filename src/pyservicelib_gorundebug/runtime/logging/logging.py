#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from .asynclog.asynclog import AsyncLogsEngine, AsyncLogger
from ..environment.log import LogsEngine


async def create_asynclog_engine() -> LogsEngine:
    """Create an AsyncLogsEngine (Python-only, queue-based async handler)."""
    return await AsyncLogsEngine.engine()

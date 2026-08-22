#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""In-memory logging engine for tests.

Usage::

    engine = TestLog()
    # pass via ServiceDependency.get_logs_engine()

    entries = engine.entries()
    assert entries[0].level == Level.ERROR
    assert "connection refused" in entries[0].message
"""

from threading import Lock
from typing import Optional

from ..environment.log import Level, Field, Logger, LogsEngine, Config


class Entry:
    """One captured log record."""
    __slots__ = ('level', 'message', 'fields')

    def __init__(self, level: Level, message: str, fields: tuple[Field, ...]) -> None:
        self.level = level
        self.message = message
        self.fields = fields

    def __repr__(self) -> str:
        return f'Entry({self.level} {self.message!r} fields={self.fields})'


class _TestLogger(Logger):
    def __init__(self, engine: 'TestLog') -> None:
        self._engine = engine

    def debug(self, msg: str, *fields: Field) -> None:
        self._engine._record(Entry(Level.DEBUG, msg, fields))

    def info(self, msg: str, *fields: Field) -> None:
        self._engine._record(Entry(Level.INFO, msg, fields))

    def warn(self, msg: str, *fields: Field) -> None:
        self._engine._record(Entry(Level.WARN, msg, fields))

    def error(self, msg: str, *fields: Field) -> None:
        self._engine._record(Entry(Level.ERROR, msg, fields))


class TestLog(LogsEngine):
    """Implements LogsEngine; all log calls are captured in memory."""

    def __init__(self) -> None:
        self._mu: Lock = Lock()
        self._entries: list[Entry] = []

    def _record(self, entry: Entry) -> None:
        with self._mu:
            self._entries.append(entry)

    def entries(self) -> list[Entry]:
        """Snapshot of all recorded entries."""
        with self._mu:
            return list(self._entries)

    def entries_at_level(self, level: Level) -> list[Entry]:
        """Snapshot filtered to a specific log level."""
        return [e for e in self.entries() if e.level == level]

    def reset(self) -> None:
        """Clear all recorded entries."""
        with self._mu:
            self._entries.clear()

    def default_logger(self, cfg: Optional[Config] = None) -> Logger:
        return _TestLogger(self)

    async def shutdown(self) -> None:
        pass

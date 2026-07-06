#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Any, Optional


# ── Level ─────────────────────────────────────────────────────────────────────

class Level(IntEnum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3

    def __str__(self) -> str:
        return self.name.lower()


# ── FieldType ─────────────────────────────────────────────────────────────────

class FieldType(IntEnum):
    STRING = 0
    INT64 = 1
    FLOAT64 = 2
    BOOL = 3
    ERROR = 4
    ANY = 5


# ── Field ─────────────────────────────────────────────────────────────────────

class Field:
    """Structured log field. Immutable; created via constructor functions below."""

    __slots__ = ('key', 'type', '_val')

    def __init__(self, key: str, ftype: FieldType, val: Any) -> None:
        self.key = key
        self.type = ftype
        self._val = val

    def str_val(self) -> str:
        return self._val if self._val is not None else ''

    def int64_val(self) -> int:
        return int(self._val) if self._val is not None else 0

    def float64_val(self) -> float:
        return float(self._val) if self._val is not None else 0.0

    def bool_val(self) -> bool:
        return bool(self._val)

    def any_val(self) -> Any:
        return self._val

    def error_val(self) -> Optional[BaseException]:
        return self._val if isinstance(self._val, BaseException) else None

    def value(self) -> Any:
        """Return field value as Any. Prefer typed accessors in performance-sensitive code."""
        if self.type == FieldType.STRING:
            return self._val if self._val is not None else ''
        if self.type == FieldType.INT64:
            return int(self._val) if self._val is not None else 0
        if self.type == FieldType.FLOAT64:
            return float(self._val) if self._val is not None else 0.0
        if self.type == FieldType.BOOL:
            return bool(self._val)
        return self._val

    def string_value(self) -> str:
        """Format field value as a string for text-only backends."""
        if self.type == FieldType.STRING:
            return self._val if self._val is not None else ''
        if self.type == FieldType.INT64:
            return str(int(self._val)) if self._val is not None else '0'
        if self.type == FieldType.FLOAT64:
            v = float(self._val) if self._val is not None else 0.0
            return f'{v:g}'
        if self.type == FieldType.BOOL:
            return 'true' if self._val else 'false'
        if self.type == FieldType.ERROR:
            return str(self._val) if self._val is not None else ''
        return str(self._val) if self._val is not None else ''

    def __repr__(self) -> str:
        return f'Field({self.key}={self.string_value()!r})'


# ── Field constructors ────────────────────────────────────────────────────────

def str_field(key: str, val: str) -> Field:
    return Field(key, FieldType.STRING, val)


def int_field(key: str, val: int) -> Field:
    return Field(key, FieldType.INT64, val)


def int64_field(key: str, val: int) -> Field:
    return Field(key, FieldType.INT64, val)


def float64_field(key: str, val: float) -> Field:
    return Field(key, FieldType.FLOAT64, val)


def bool_field(key: str, val: bool) -> Field:
    return Field(key, FieldType.BOOL, val)


def err_field(err: Optional[BaseException]) -> Field:
    return Field('error', FieldType.ERROR, err)


def any_field(key: str, val: Any) -> Field:
    return Field(key, FieldType.ANY, val)


# ── Config ────────────────────────────────────────────────────────────────────

class Config:
    pass


# ── Logger / LogsEngine ───────────────────────────────────────────────────────

class Logger(ABC):
    """Structured logger. Pass key-value pairs as Field values using
    the constructors (str_field, int_field, err_field, …).
    No format strings, no eager str interpolation."""

    @abstractmethod
    def debug(self, msg: str, *fields: Field) -> None: ...

    @abstractmethod
    def info(self, msg: str, *fields: Field) -> None: ...

    @abstractmethod
    def warn(self, msg: str, *fields: Field) -> None: ...

    @abstractmethod
    def error(self, msg: str, *fields: Field) -> None: ...


class LogsEngine(ABC):

    @abstractmethod
    def default_logger(self, cfg: Optional[Config] = None) -> Logger: ...

    @abstractmethod
    async def shutdown(self) -> None: ...

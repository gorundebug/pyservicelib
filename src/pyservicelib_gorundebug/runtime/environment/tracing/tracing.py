#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, MutableMapping, Optional, Tuple


# ── Attribute ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class Attribute:
    key: str
    value: Any


def string_attr(key: str, value: str) -> Attribute:
    return Attribute(key=key, value=value)


def int64_attr(key: str, value: int) -> Attribute:
    return Attribute(key=key, value=value)


def float64_attr(key: str, value: float) -> Attribute:
    return Attribute(key=key, value=value)


def bool_attr(key: str, value: bool) -> Attribute:
    return Attribute(key=key, value=value)


# ── SpanContext ───────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class SpanContext:
    trace_id: str = ''
    span_id: str = ''
    is_valid: bool = False


# ── StatusCode ────────────────────────────────────────────────────────────────

class StatusCode:
    UNSET = 0
    OK = 1
    ERROR = 2


# ── Span ──────────────────────────────────────────────────────────────────────

class Span(ABC):
    @abstractmethod
    def end(self) -> None: ...

    @abstractmethod
    def set_attributes(self, *attrs: Attribute) -> None: ...

    @abstractmethod
    def record_error(self, err: Exception) -> None: ...

    @abstractmethod
    def set_status(self, code: int, description: str) -> None: ...

    @abstractmethod
    def add_event(self, name: str, *attrs: Attribute) -> None: ...

    @abstractmethod
    def span_context(self) -> SpanContext: ...

    @contextmanager
    def scoped(self):
        """Set this span as the current span for the duration of the block.
        Child spans created within the block will be linked to this span."""
        yield self


# ── Noop span ─────────────────────────────────────────────────────────────────

class _NoopSpan(Span):
    def end(self) -> None: pass
    def set_attributes(self, *attrs: Attribute) -> None: pass
    def record_error(self, err: Exception) -> None: pass
    def set_status(self, code: int, description: str) -> None: pass
    def add_event(self, name: str, *attrs: Attribute) -> None: pass
    def span_context(self) -> SpanContext: return SpanContext()


NOOP_SPAN: Span = _NoopSpan()


# ── Tracer ────────────────────────────────────────────────────────────────────

class Tracer(ABC):
    @abstractmethod
    def start(self, span_name: str, *attrs: Attribute) -> Tuple[Any, Span]: ...


# ── Tracing ───────────────────────────────────────────────────────────────────

class Tracing(ABC):
    @abstractmethod
    def tracer(self, name: str) -> Tracer: ...

    @contextmanager
    def extract(self, carrier: Mapping[str, str]) -> Iterator[bool]:
        """Activate a remote transport context and yield whether it is sampled."""
        del carrier
        yield False

    def inject(self, carrier: MutableMapping[str, str]) -> None:
        """Inject the current span context into a transport carrier."""
        del carrier


# ── TracingEngine ─────────────────────────────────────────────────────────────

class TracingEngine(ABC):
    @property
    @abstractmethod
    def tracing(self) -> Tracing: ...

    async def shutdown(self) -> None:
        pass


# ── Context sampling ──────────────────────────────────────────────────────────

_sampling_var: ContextVar[bool] = ContextVar('_tracing_sampling', default=False)


def enable_sampling() -> None:
    """Mark the current coroutine context for tracing."""
    _sampling_var.set(True)


@contextmanager
def sampling_scope(enabled: bool) -> Iterator[None]:
    token = _sampling_var.set(enabled)
    try:
        yield
    finally:
        _sampling_var.reset(token)


def sampling_enabled() -> bool:
    """Return True if the current coroutine context has tracing enabled."""
    return _sampling_var.get()


# ── Helpers ───────────────────────────────────────────────────────────────────

def start_span(tracer: Optional[Tracer], operation: str, *attrs: Attribute) -> Tuple[Any, Span]:
    """Start a span. Returns (ctx, noop_span) when tracer is None or sampling is off."""
    if tracer is None or not sampling_enabled():
        return None, NOOP_SPAN
    return tracer.start(operation, *attrs)


def span_event(span: Optional[Span], name: str, *attrs: Attribute) -> None:
    if span is not None:
        span.add_event(name, *attrs)


def span_error(span: Optional[Span], err: Exception) -> None:
    if span is not None:
        span.record_error(err)
        span.set_status(StatusCode.ERROR, str(err))


def span_attrs(span: Optional[Span], *attrs: Attribute) -> None:
    if span is not None:
        span.set_attributes(*attrs)

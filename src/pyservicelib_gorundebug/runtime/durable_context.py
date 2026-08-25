#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Processing-side state for one Temporal endpoint Activity.

The live object never crosses a Workflow or process boundary. Its presence
only means that the current graph execution entered through a Temporal source.
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token
from typing import Any, Final

from .environment.tracing import Span, StatusCode, string_attr


class DurableCallContextError(RuntimeError):
    """Base error for processing-side Temporal Activity misuse."""


class DurableCallHeartbeatAfterCompletionError(DurableCallContextError):
    pass


HEARTBEAT: Final = "heartbeat"
SUCCESS: Final = "success"
ERROR: Final = "error"
LATE_HEARTBEAT: Final = "late_heartbeat"

type DurableCallDiagnostics = Callable[[str, BaseException | None], None]
type DurableCallHeartbeatRecorder = Callable[[Any], None]


class DurableCallContext:
    """Thread-safe Activity state shared by ordinary downstream graph code."""

    __slots__ = (
        "message_id",
        "_lock",
        "_closed",
        "_heartbeat",
        "_diagnostics",
        "_span",
        "_span_ended",
    )

    def __init__(
        self,
        message_id: str,
        heartbeat: DurableCallHeartbeatRecorder | None = None,
        diagnostics: DurableCallDiagnostics | None = None,
    ) -> None:
        self.message_id = message_id
        self._lock = threading.Lock()
        self._closed = False
        self._heartbeat = heartbeat
        self._diagnostics = diagnostics
        self._span: Span | None = None
        self._span_ended = False

    def bind_span(self, span: Span) -> None:
        with self._lock:
            self._span = span

    def _report(self, event: str, error: BaseException | None) -> None:
        with self._lock:
            span = self._span
        if span is not None:
            attrs = [string_attr("event", event)]
            if error is not None:
                attrs.append(string_attr("error", str(error)))
            span.add_event(f"temporal.activity.{event}", *attrs)
            if event == ERROR:
                recorded = (
                    error
                    if isinstance(error, Exception)
                    else RuntimeError(str(error or event))
                )
                span.record_error(recorded)
                span.set_status(StatusCode.ERROR, str(error or event))
        if self._diagnostics is not None:
            self._diagnostics(event, error)

    def heartbeat(self, message: Any) -> None:
        with self._lock:
            if self._closed:
                recorder = None
                error = DurableCallHeartbeatAfterCompletionError(
                    "durable call heartbeat after completion"
                )
            else:
                recorder = self._heartbeat
                error = None
        if error is not None:
            self._report(LATE_HEARTBEAT, error)
            raise error
        if recorder is not None:
            recorder(message)
        self._report(HEARTBEAT, None)

    def close(self, outcome: BaseException | None) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            span = self._span
            should_end_span = span is not None and not self._span_ended
            if should_end_span:
                self._span_ended = True
        event = ERROR if outcome is not None else SUCCESS
        self._report(event, outcome)
        if should_end_span and span is not None:
            if outcome is None:
                span.set_status(StatusCode.OK, "")
            span.end()


_current_durable_call: ContextVar[DurableCallContext | None] = ContextVar(
    "_current_durable_call", default=None
)


def current_durable_call_context() -> DurableCallContext | None:
    return _current_durable_call.get()


def durable_call_heartbeat(message: Any) -> None:
    """Record Activity progress, or silently do nothing outside Temporal."""

    durable = current_durable_call_context()
    if durable is not None:
        durable.heartbeat(message)


def bind_durable_call_span(span: Span) -> bool:
    durable = current_durable_call_context()
    if durable is None:
        return False
    durable.bind_span(span)
    return True


async def run_durable_call_activity[ResultT](
    durable: DurableCallContext,
    invoke: Callable[[], Awaitable[ResultT]],
) -> ResultT:
    """Run one existing endpoint handler inside its Activity scope."""

    token: Token[DurableCallContext | None] = _current_durable_call.set(durable)
    try:
        try:
            result = await invoke()
        except BaseException as error:
            durable.close(error)
            raise
        durable.close(None)
        return result
    finally:
        _current_durable_call.reset(token)

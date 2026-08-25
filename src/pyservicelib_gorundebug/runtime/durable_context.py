#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Processing-side DurableCall Activity lifecycle.

The live object in this module is local to one Activity execution. It is never
serialized and never crosses the Temporal boundary.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from .environment.tracing import Span, StatusCode, string_attr


class DurableCallContextError(RuntimeError):
    """Base error for observable DurableCall lifecycle misuse."""


class NoDurableCallContextError(DurableCallContextError):
    pass


class DurableCallAlreadyCompletedError(DurableCallContextError):
    pass


class DurableCallHeartbeatAfterCompletionError(DurableCallContextError):
    pass


class DurableCallOutcomeMissingError(DurableCallContextError):
    pass


HEARTBEAT: Final = "heartbeat"
SUCCESS: Final = "success"
ERROR: Final = "error"
MISSING_OUTCOME: Final = "missing_outcome"
DUPLICATE_TERMINAL: Final = "duplicate_terminal"
LATE_HEARTBEAT: Final = "late_heartbeat"
SUSPENDED: Final = "suspended"


@dataclass(frozen=True, slots=True)
class DurableContinuation:
    version: int
    from_name: str
    to_name: str
    call_id: str
    stream_id: str
    priority: int
    deadline_unix_nano: int
    wake_at_unix_nano: int
    payload: bytes
    trace_carrier: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DurableActivityResult:
    continuation: DurableContinuation | None = None

type DurableCallDiagnostics = Callable[[str, BaseException | None], None]
type DurableCallHeartbeatRecorder = Callable[[Any], None]


class DurableCallContext:
    """Thread-safe state shared by graph branches for one Activity."""

    __slots__ = (
        "parent_id", "counts", "_lock", "_completed", "_outcome", "_done",
        "_heartbeat", "_diagnostics", "_span", "_span_ended",
        "_delay_at", "_continuation",
    )

    def __init__(
        self,
        parent_id: str,
        heartbeat: DurableCallHeartbeatRecorder | None = None,
        diagnostics: DurableCallDiagnostics | None = None,
    ) -> None:
        self.parent_id = parent_id
        self.counts: dict[bytes, int] = {}
        self._lock = threading.Lock()
        self._completed = False
        self._outcome: BaseException | None = None
        self._done = asyncio.Event()
        self._heartbeat = heartbeat
        self._diagnostics = diagnostics
        self._span: Span | None = None
        self._span_ended = False
        self._delay_at: datetime | None = None
        self._continuation: DurableContinuation | None = None

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
            span.add_event(f"durable_call.{event}", *attrs)
            if event in (ERROR, MISSING_OUTCOME):
                recorded_error = (
                    error if isinstance(error, Exception) else RuntimeError(str(error or event))
                )
                span.record_error(recorded_error)
                span.set_status(StatusCode.ERROR, str(error or event))
        if self._diagnostics is not None:
            self._diagnostics(event, error)

    def _complete(self, event: str, outcome: BaseException | None) -> None:
        with self._lock:
            if self._completed:
                error = DurableCallAlreadyCompletedError(
                    f"durable call is already completed; attempted {event}"
                )
            else:
                self._completed = True
                self._outcome = outcome
                error = None
        if error is not None:
            self._report(DUPLICATE_TERMINAL, error)
            raise error
        self._report(event, outcome)
        self._done.set()

    def heartbeat(self, message: Any) -> None:
        with self._lock:
            if self._completed:
                error = DurableCallHeartbeatAfterCompletionError(
                    "durable call heartbeat after completion"
                )
            else:
                error = None
                if self._heartbeat is not None:
                    self._heartbeat(message)
        if error is not None:
            self._report(LATE_HEARTBEAT, error)
            raise error
        self._report(HEARTBEAT, None)

    def success(self) -> None:
        self._complete(SUCCESS, None)

    def fail(self, error: BaseException) -> None:
        self._complete(ERROR, error)

    def cancel_without_outcome(self, cause: BaseException | None) -> None:
        error = DurableCallOutcomeMissingError(
            "durable call completed without explicit outcome"
            + (f": {cause}" if cause is not None else "")
        )
        if cause is not None:
            error.__cause__ = cause
        with self._lock:
            if self._completed:
                return
            self._completed = True
            self._outcome = error
        self._report(MISSING_OUTCOME, error)
        self._done.set()

    async def wait(self) -> DurableActivityResult:
        await self._done.wait()
        with self._lock:
            outcome = self._outcome
            continuation = self._continuation
        if outcome is not None:
            raise outcome
        return DurableActivityResult(continuation)

    def begin_delay(self, duration: timedelta) -> None:
        with self._lock:
            if self._completed:
                raise DurableCallAlreadyCompletedError(
                    "durable call is already completed; attempted delay"
                )
            if self._delay_at is not None:
                raise DurableCallContextError("durable delay is already pending")
            self._delay_at = datetime.now(timezone.utc) + duration

    def capture_continuation(
        self, from_name: str, to_name: str, payload: bytes,
        trace_carrier: Mapping[str, str] | None = None,
    ) -> bool:
        from .context import priority_from_context, request_deadline, stream_id_from_context

        with self._lock:
            if self._delay_at is None:
                return False
            if self._completed:
                raise DurableCallAlreadyCompletedError(
                    "durable call is already completed; attempted suspension"
                )
            deadline = request_deadline.get()
            if deadline is not None and deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            self._continuation = DurableContinuation(
                version=1,
                from_name=from_name,
                to_name=to_name,
                call_id=f"{self.parent_id}/delay",
                stream_id=stream_id_from_context() or "",
                priority=priority_from_context() or 0,
                deadline_unix_nano=(
                    int(deadline.timestamp() * 1_000_000_000)
                    if deadline is not None else 0
                ),
                wake_at_unix_nano=int(self._delay_at.timestamp() * 1_000_000_000),
                payload=bytes(payload),
                trace_carrier=dict(trace_carrier or {}),
            )
            self._completed = True
        self._report(SUSPENDED, None)
        self._done.set()
        return True

    def finish_span(self) -> None:
        with self._lock:
            if self._span is None or self._span_ended:
                return
            self._span_ended = True
            span = self._span
            outcome = self._outcome
        if outcome is None:
            span.set_status(StatusCode.OK, "")
        span.end()


_current_durable_call: ContextVar[DurableCallContext | None] = ContextVar(
    "_current_durable_call", default=None
)


def current_durable_call_context() -> DurableCallContext | None:
    return _current_durable_call.get()


def begin_durable_delay(duration: timedelta) -> bool:
    durable = current_durable_call_context()
    if durable is None:
        return False
    durable.begin_delay(duration)
    return True


def capture_durable_continuation(
    from_name: str, to_name: str, payload: bytes,
    trace_carrier: Mapping[str, str] | None = None,
) -> bool:
    durable = current_durable_call_context()
    if durable is None:
        return False
    return durable.capture_continuation(
        from_name, to_name, payload, trace_carrier
    )


def _require_current(operation: str) -> DurableCallContext:
    durable = current_durable_call_context()
    if durable is None:
        error = NoDurableCallContextError(
            f"DurableCall {operation} invoked outside an Activity"
        )
        logging.getLogger("pyservicelib.durable_call").warning(
            "DurableCall operation invoked outside an Activity",
            extra={"operation": operation, "error": str(error)},
        )
        raise error
    return durable


def durable_call_heartbeat(message: Any) -> None:
    _require_current(HEARTBEAT).heartbeat(message)


def durable_call_success() -> None:
    _require_current(SUCCESS).success()


def durable_call_error(error: BaseException) -> None:
    if error is None:
        raise ValueError("durable_call_error requires a non-null error")
    _require_current(ERROR).fail(error)


def bind_durable_call_span(span: Span) -> bool:
    durable = current_durable_call_context()
    if durable is None:
        return False
    durable.bind_span(span)
    return True


async def run_durable_call_activity(
    durable: DurableCallContext,
    invoke: Callable[[], Any],
) -> DurableActivityResult:
    """Dispatch existing graph code, then await explicit outcome/cancellation."""

    token: Token[DurableCallContext | None] = _current_durable_call.set(durable)
    try:
        try:
            try:
                result = invoke()
                if hasattr(result, "__await__"):
                    await result
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                try:
                    durable.fail(error)
                except DurableCallAlreadyCompletedError:
                    pass
            return await durable.wait()
        except asyncio.CancelledError as cause:
            durable.cancel_without_outcome(cause)
            raise DurableCallOutcomeMissingError(
                "durable call completed without explicit outcome: Activity cancelled"
            ) from cause
    finally:
        durable.finish_span()
        _current_durable_call.reset(token)

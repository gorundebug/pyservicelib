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
from typing import Any, Final, NoReturn

from .environment.tracing import Span, StatusCode, string_attr
from .execution_policy import recording_policy_scope


class DurableCallContextError(RuntimeError):
    """Base error for processing-side Temporal Activity misuse."""


class DurableCallHeartbeatAfterCompletionError(DurableCallContextError):
    pass


class TemporalContinueAsNewRequest(BaseException):
    """Terminal Workflow control outcome consumed by the Temporal adapter."""

    def __init__(self, next_input: Any) -> None:
        super().__init__("Temporal Continue-As-New")
        self.next_input = next_input


HEARTBEAT: Final = "heartbeat"
SUCCESS: Final = "success"
ERROR: Final = "error"
LATE_HEARTBEAT: Final = "late_heartbeat"

type DurableCallDiagnostics = Callable[[str, BaseException | None], None]
type DurableCallHeartbeatRecorder = Callable[[Any], None]
type DurableCallDelay = Callable[[Any], Awaitable[None]]


class _WorkflowLock:
    """No-op context manager for Temporal's single-threaded Workflow isolate."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: Any) -> None:
        return None


class DurableCallContext:
    """Thread-safe Activity state shared by ordinary downstream graph code."""

    __slots__ = (
        "message_id",
        "_lock",
        "_closed",
        "_heartbeat",
        "_delay",
        "_workflow",
        "_recording_policy",
        "_diagnostics",
        "_span",
        "_span_ended",
    )

    def __init__(
        self,
        message_id: str,
        heartbeat: DurableCallHeartbeatRecorder | None = None,
        diagnostics: DurableCallDiagnostics | None = None,
        delay: DurableCallDelay | None = None,
        workflow: bool = False,
        recording_policy: Callable[[], bool] | None = None,
    ) -> None:
        self.message_id = message_id
        self._lock = _WorkflowLock() if workflow else threading.Lock()
        self._closed = False
        self._heartbeat = heartbeat
        self._delay = delay
        self._workflow = workflow
        self._recording_policy = recording_policy
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
            attrs = []
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
        if self._workflow:
            return
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

    async def delay(self, duration: Any) -> bool:
        if self._delay is None:
            return False
        await self._delay(duration)
        return True

    def continue_as_new(self, next_input: Any) -> NoReturn:
        with self._lock:
            workflow = self._workflow
            closed = self._closed
        if not workflow:
            raise DurableCallContextError(
                "Temporal Continue-As-New requires a Workflow endpoint"
            )
        if closed:
            raise DurableCallContextError(
                "Temporal Continue-As-New after Workflow completion"
            )
        raise TemporalContinueAsNewRequest(next_input)

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


def temporal_continue_as_new(next_input: Any) -> NoReturn:
    """Terminate the current Workflow run with a new typed endpoint input."""

    durable = current_durable_call_context()
    if durable is None:
        raise DurableCallContextError(
            "Temporal Continue-As-New requires a Workflow endpoint"
        )
    durable.continue_as_new(next_input)


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


async def durable_call_delay(duration: Any) -> bool:
    """Wait durably in a Workflow, or return False for the local backend."""

    durable = current_durable_call_context()
    return await durable.delay(duration) if durable is not None else False


async def run_durable_call_workflow[ResultT](
    durable: DurableCallContext,
    invoke: Callable[[], Awaitable[ResultT]],
) -> ResultT:
    """Run one existing endpoint handler inside its Workflow scope."""

    async def run() -> ResultT:
        token: Token[DurableCallContext | None] = _current_durable_call.set(durable)
        try:
            try:
                result = await invoke()
            except TemporalContinueAsNewRequest:
                durable.close(None)
                raise
            except BaseException as error:
                durable.close(error)
                raise
            durable.close(None)
            return result
        finally:
            _current_durable_call.reset(token)

    policy = durable._recording_policy
    if policy is None:
        return await run()
    with recording_policy_scope(policy):
        return await run()

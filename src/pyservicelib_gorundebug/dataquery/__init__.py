"""Independent Query request lifecycle; no Sink, private queue or detached task."""
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Protocol

from .metrics import EndpointMetrics
from ..runtime.execution_policy import recording_enabled

from ..runtime.common import Collect, CollectFunc, Consumer, Stream, TypedQueryStream
from ..runtime.context.request import request_context_error, request_cancelled, request_deadline


@asynccontextmanager
async def _request_execution_scope(operation_timeout_ms: int = 0):
    """Interrupt native I/O with the inherited request deadline/cancellation.

    The short-lived observer is joined before returning; execution stays in the
    caller task and preserves contextvars. It is not a detached delivery task.
    """
    owner = asyncio.current_task()
    signal = request_cancelled.get()
    deadline = request_deadline.get()
    if signal is not None and signal.is_set():
        raise asyncio.CancelledError("request cancelled")
    timeout = None
    if deadline is not None:
        now = datetime.now(timezone.utc if deadline.tzinfo is not None else None)
        timeout = (deadline - now).total_seconds()
        if timeout <= 0:
            raise TimeoutError("deadline exceeded")

    if operation_timeout_ms:
        limit = operation_timeout_ms / 1000
        timeout = limit if timeout is None else min(timeout, limit)

    async def observe_cancel():
        await signal.wait()
        owner.cancel()

    observer = asyncio.create_task(observe_cancel()) if signal is not None else None
    expires_at = asyncio.get_running_loop().time() + timeout if timeout is not None else None
    try:
        async with asyncio.timeout(timeout):
            yield
            # A mapper can return without yielding to the event loop. Check the
            # outcome before cancelling the observer/timer during cleanup.
            if signal is not None and signal.is_set():
                raise asyncio.CancelledError("request cancelled")
            if expires_at is not None and asyncio.get_running_loop().time() >= expires_at:
                raise TimeoutError("deadline exceeded")
    finally:
        if observer is not None:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)


@dataclass(frozen=True)
class BeginRequest[S]:
    state: S
    error: Exception | None = None


class EndpointHandler[S, R, E](Protocol):
    async def begin_request(self, stream: Stream) -> BeginRequest[S]: ...

    async def end_request(
        self, stream: Stream, state: S | None, error: BaseException | None,
        out: Collect[R], errors: Collect[E],
    ) -> None:
        """Map errors and optionally emit application completion via ordinary out.

        State is None only when begin_request raises before returning it.
        Collectors must not be retained beyond this invocation.
        """
        ...


class _EndpointConsumer[S, T, R, E](Consumer[T]):
    def __init__(
        self, stream: TypedQueryStream[T, R, E], handler: EndpointHandler[S, R, E],
        execute: Callable[[S, T, Collect[R]], Awaitable[None]],
        operation_timeout_ms: int = 0,
    ):
        self._metrics = EndpointMetrics(stream) if stream.environment.metrics.enabled else None
        self._stream = stream
        self._handler = handler
        self._execute = execute
        if isinstance(operation_timeout_ms, bool) or not isinstance(operation_timeout_ms, int) or operation_timeout_ms < 0:
            raise ValueError("Query operation timeout must be a nonnegative integer")
        self._operation_timeout_ms = operation_timeout_ms

    async def consume(self, value: T) -> None:
        started = self._metrics.begin() if self._metrics is not None else None
        async def emit(result):
            if recording_enabled():
                self._metrics.results.inc()
            await self._stream.consume_result(result)
        out = CollectFunc[R](emit if started is not None else self._stream.consume_result)
        errors = CollectFunc[E](self._stream.error_stream.consume)
        state: S | None = None
        error: BaseException | None = None
        cancelled: asyncio.CancelledError | None = None
        context_cancelled = False
        try:
            # Use the ambient SDK request context unchanged, including its deadline
            # and cancellation signal. Native adapters own interruptible driver I/O.
            try:
                begun = await self._handler.begin_request(self._stream)
                state, error = begun.state, begun.error
                if error is None and (reason := request_context_error()) is not None:
                    error = RuntimeError(reason)
                    context_cancelled = True
                if error is None:
                    async with _request_execution_scope(self._operation_timeout_ms):
                        await self._execute(begun.state, value, out)
            except asyncio.CancelledError as exc:
                error = cancelled = exc
            except Exception as exc:
                error = exc
            # Adapter Execute must close its cursor/body before unwinding here.
            # No automatic completion result is generated.
            await self._handler.end_request(self._stream, state, error, out, errors)
            if cancelled is not None:
                raise cancelled
        finally:
            if self._metrics is not None:
                self._metrics.finish(started, error, context_cancelled)


def make_endpoint_consumer[S, T, R, E](
    stream: TypedQueryStream[T, R, E], handler: EndpointHandler[S, R, E],
    execute: Callable[[S, T, Collect[R]], Awaitable[None]],
    *, operation_timeout_ms: int = 0,
) -> Consumer[T]:
    """Bind native execution; resource startup/stop belongs to the connector.

    Execute emits mapped results and releases all native resources before return.
    It must not retry after partial output, retain collectors or detach work.
    """
    consumer = _EndpointConsumer(stream, handler, execute, operation_timeout_ms)
    stream.set_query_consumer(consumer)
    return consumer

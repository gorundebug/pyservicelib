#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import os
from contextvars import ContextVar
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from .context import Context

# Per-request deadline, analogous to ctx.Deadline() in Go.
# Set by datasource handlers at request entry; automatically propagated by asyncio
# to all coroutines spawned within the same task context.
request_deadline: ContextVar[Optional[datetime]] = ContextVar('request_deadline', default=None)

# Per-request cancellation signal, analogous to ctx.Done() in Go.
# Set by datasource handlers at request entry; call .set() on the event to cancel
# (e.g. on HTTP client disconnect). Pool operations check this before accepting
# or executing tasks — mirrors ctx.Err() check in Go's Delay().
request_cancelled: ContextVar[Optional[asyncio.Event]] = ContextVar('request_cancelled', default=None)


class _ContextCancellationEvent(asyncio.Event):
    """Retain the owner across queued tasks, subscribing only while awaited."""

    def __init__(self, owner: Context):
        super().__init__()
        self._owner = owner

    def is_set(self) -> bool:
        return super().is_set() or self._owner.cancelled

    async def wait(self) -> bool:
        if self.is_set():
            return True
        loop = asyncio.get_running_loop()

        def notify() -> None:
            try:
                current = asyncio.get_running_loop()
            except RuntimeError:
                current = None
            if current is loop:
                self.set()
            elif not loop.is_closed():
                loop.call_soon_threadsafe(self.set)

        unsubscribe = self._owner.add_cancel_callback(notify)
        try:
            return await super().wait()
        finally:
            unsubscribe()


@contextmanager
def request_context_scope(owner: Context):
    """Bind an explicit owner to the request variables used by operators/pools."""
    remaining = owner.time_left
    deadline = (
        None if remaining is None
        else datetime.now(timezone.utc) + timedelta(seconds=remaining)
    )
    deadline_token = request_deadline.set(deadline)
    cancel_token = request_cancelled.set(_ContextCancellationEvent(owner))
    stream_token = request_stream_id.set(request_stream_id.get())
    try:
        yield
    finally:
        request_stream_id.reset(stream_token)
        request_cancelled.reset(cancel_token)
        request_deadline.reset(deadline_token)


def request_context_error() -> Optional[str]:
    """Return the Python equivalent of Go's ctx.Err() for the current request."""
    cancelled = request_cancelled.get()
    if cancelled is not None and cancelled.is_set():
        return "request cancelled"

    deadline = request_deadline.get()
    if deadline is None:
        return None
    now = datetime.now(
        timezone.utc if deadline.tzinfo is not None else None
    )
    if deadline <= now:
        return "deadline exceeded"
    return None

# Per-request stream ID, analogous to runtime.WithStreamId/StreamIdFromContext in Go.
# Used to correlate async pipeline results back to the originating request.
# Propagated to asyncio tasks via copy_context() at create_task() time.
request_stream_id: ContextVar[Optional[str]] = ContextVar('request_stream_id', default=None)


def new_stream_id() -> str:
    raw = os.urandom(16)
    value = raw.hex()
    return (f"{value[:8]}-{value[8:12]}-4{value[13:16]}-"
            f"{'89ab'[raw[8] & 3]}{value[17:20]}-{value[20:]}")


def stream_id_from_context() -> Optional[str]:
    return request_stream_id.get()


def with_stream_id(sid: str) -> str:
    request_stream_id.set(sid)
    return sid


# Per-request priority, analogous to runtime.WithPriority/PriorityFromContext in Go.
# Set by callers that want to override the link-configured priority for a single message.
request_priority: ContextVar[Optional[int]] = ContextVar('request_priority', default=None)


def with_priority(priority: int) -> None:
    request_priority.set(priority)


def priority_from_context() -> Optional[int]:
    return request_priority.get()

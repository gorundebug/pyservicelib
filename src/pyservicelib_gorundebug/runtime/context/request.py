#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import uuid
from contextvars import ContextVar
from datetime import datetime
from typing import Optional

# Per-request deadline, analogous to ctx.Deadline() in Go.
# Set by datasource handlers at request entry; automatically propagated by asyncio
# to all coroutines spawned within the same task context.
request_deadline: ContextVar[Optional[datetime]] = ContextVar('request_deadline', default=None)

# Per-request cancellation signal, analogous to ctx.Done() in Go.
# Set by datasource handlers at request entry; call .set() on the event to cancel
# (e.g. on HTTP client disconnect). Pool operations check this before accepting
# or executing tasks — mirrors ctx.Err() check in Go's Delay().
request_cancelled: ContextVar[Optional[asyncio.Event]] = ContextVar('request_cancelled', default=None)

# Per-request stream ID, analogous to runtime.WithStreamId/StreamIdFromContext in Go.
# Used to correlate async pipeline results back to the originating request.
# Propagated to asyncio tasks via copy_context() at create_task() time.
request_stream_id: ContextVar[Optional[str]] = ContextVar('request_stream_id', default=None)


def new_stream_id() -> str:
    return str(uuid.uuid4())


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

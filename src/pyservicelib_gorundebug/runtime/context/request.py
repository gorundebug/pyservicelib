#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
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

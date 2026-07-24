from datetime import datetime, timedelta, timezone

from pyservicelib_gorundebug.runtime.common import Collect, Stream
from pyservicelib_gorundebug.runtime.context.request import request_deadline

from ..types import Order


class SoftDeadline:
    def __init__(self, safety_margin: timedelta):
        self._safety_margin = safety_margin

    async def duration(self, stream: Stream, value: Order) -> timedelta:
        del stream, value
        deadline = request_deadline.get()
        if deadline is None:
            return self._safety_margin
        remaining = deadline - datetime.now(timezone.utc) - self._safety_margin
        return max(remaining, timedelta())

    async def delay_error(
        self,
        stream: Stream,
        value: Order,
        error: Exception,
        out: Collect[Order],
    ) -> None:
        del stream, error
        await out.out(value)

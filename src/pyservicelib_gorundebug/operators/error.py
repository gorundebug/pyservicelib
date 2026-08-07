#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from ..runtime.common import TypedConsumedStream, Collect, ServiceExecutionEnvironment
from ..runtime.serde import TypedStreamSerde


class ErrorStream[E](TypedConsumedStream[E], Collect[E]):
    """Virtual error-output stream used by operators with a typed error channel."""

    def __init__(self, stream_id: int, env: ServiceExecutionEnvironment, serde: TypedStreamSerde[E]):
        super().__init__(stream_id=stream_id, env=env, serde=serde)

    @property
    def name(self) -> str:
        return f"error:{super().name}"

    async def consume(self, value: E) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    async def out(self, value: E) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

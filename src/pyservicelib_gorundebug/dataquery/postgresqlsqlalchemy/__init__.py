"""SQLAlchemy Core Query adapter using an async connector-owned engine.

Statements are SQLAlchemy expressions or text() with separate bound parameters.
No ORM session, delivery queue, automatic replay or schema creation is introduced.
See https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.engine import CursorResult, Row
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Executable

from .. import EndpointHandler, make_endpoint_consumer
from ...runtime.common import Collect, Consumer, TypedQueryStream
from ...runtime.context.request import request_context_error


@dataclass(frozen=True)
class Query:
    statement: Executable
    parameters: Mapping[str, Any] | None = None


class RowsHandler[S, T, R, E](EndpointHandler[S, R, E], Protocol):
    async def build_query(self, state: S, value: T) -> Query: ...
    async def handle_row(self, state: S, row: Row[Any], out: Collect[R]) -> None: ...


class ExecuteHandler[S, T, R, E](EndpointHandler[S, R, E], Protocol):
    async def build_query(self, state: S, value: T) -> Query: ...
    async def handle_result(self, state: S, result: CursorResult[Any], out: Collect[R]) -> None: ...


def _check_request() -> None:
    if (reason := request_context_error()) is not None:
        raise RuntimeError(reason)


def make_rows_endpoint_consumer[S, T, R, E](
    stream: TypedQueryStream[T, R, E], engine: AsyncEngine,
    handler: RowsHandler[S, T, R, E], *, fetch_size: int = 256, operation_timeout_ms: int = 0,
) -> Consumer[T]:
    """Iterate a server-side cursor; never collect the complete selection.

    The driver cursor requires a transaction. Successful iteration commits it;
    an exception rolls it back. Already emitted results cannot be retracted.
    Cursor and connection are released before EndRequest, including cancellation.
    """
    if isinstance(fetch_size, bool) or not isinstance(fetch_size, int) or fetch_size <= 0:
        raise ValueError("Query fetch_size must be a positive integer")

    async def execute(state: S, value: T, out: Collect[R]) -> None:
        query = await handler.build_query(state, value)
        _check_request()
        async with engine.begin() as connection:
            async with connection.stream(
                query.statement, query.parameters,
                execution_options={"yield_per": fetch_size},
            ) as rows:
                async for row in rows:
                    _check_request()
                    await handler.handle_row(state, row, out)
                _check_request()

    return make_endpoint_consumer(stream, handler, execute, operation_timeout_ms=operation_timeout_ms)


def make_execute_endpoint_consumer[S, T, R, E](
    stream: TypedQueryStream[T, R, E], engine: AsyncEngine,
    handler: ExecuteHandler[S, T, R, E], *, operation_timeout_ms: int = 0,
) -> Consumer[T]:
    """Commit once before mapping a result; never retry an ambiguous write.

    Execute is for bounded command results. Use Rows for streaming selections.
    Result remains usable only for handle_result and closes before EndRequest.
    """
    async def execute(state: S, value: T, out: Collect[R]) -> None:
        query = await handler.build_query(state, value)
        _check_request()
        result: CursorResult[Any] | None = None
        try:
            async with engine.begin() as connection:
                result = await connection.execute(query.statement, query.parameters)
                _check_request()
            await handler.handle_result(state, result, out)
        finally:
            if result is not None:
                result.close()

    return make_endpoint_consumer(stream, handler, execute, operation_timeout_ms=operation_timeout_ms)

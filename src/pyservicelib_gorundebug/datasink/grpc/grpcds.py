#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

"""
gRPC datasink endpoint consumers for all four streaming modes.

Equivalent to Go's datasink/grpc/ package (grpc.go + nostreaming.go +
serverstreaming.go + clientstreaming.go + bidistreaming.go).

Each make_* function takes a typed gRPC client function (matching the
protoc-generated stub signature) and returns a Consumer[T] that calls it
per pipeline value.
"""

import asyncio
from collections.abc import AsyncIterator, Generator
from datetime import datetime, timezone
from typing import Optional, Protocol, Any, cast

from ...runtime.common import (
    Consumer, SinkStreamContext, CollectFunc, TypedSinkStreamWithResult,
    ServiceExecutionEnvironment, SinkEndpoint, OutputEndpointConsumer,
)
from ...runtime.context import Context
from ...runtime.context.request import (
    new_stream_id, request_deadline, request_stream_id,
    stream_id_from_context, with_stream_id,
)
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint
from ...runtime.environment.tracing import (
    Tracer, Tracing, Span, start_endpoint_span, span_event, span_error, string_attr,
    sampling_enabled,
)
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.utils.asyncrwlock import AsyncRWLock


# ---------------------------------------------------------------------------
# gRPC stream Protocols — minimal interfaces satisfied by grpc.aio call objects.
# Port of Go's ServerStreamingGRPCStream / ClientStreamingGRPCStream /
# BidiStreamingGRPCStream interfaces in datasink/grpc/.
# ---------------------------------------------------------------------------

class ServerStreamingGRPCStream[ResR](Protocol):
    """One request → async stream of responses."""
    def __aiter__(self) -> AsyncIterator[ResR]: ...
    async def __anext__(self) -> ResR: ...


class ClientStreamingGRPCStream[ReqT, ResR](Protocol):
    """Async request stream → one response.
    Awaiting the object returns the server response (ResR)."""
    async def write(self, req: ReqT) -> None: ...
    async def done_writing(self) -> None: ...
    def __await__(self) -> Generator[Any, None, ResR]: ...


class BidiStreamingGRPCStream[ReqT, ResR](Protocol):
    """Async request stream ↔ async response stream."""
    async def write(self, req: ReqT) -> None: ...
    async def done_writing(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[ResR]: ...
    async def __anext__(self) -> ResR: ...


# ---------------------------------------------------------------------------
# Client function Protocols — match protoc-generated stub signatures.
# Port of Go's NoStreamingClientFunction / ServerStreamingClientFunction /
# ClientStreamingClientFunction / BidiStreamingClientFunction.
# ---------------------------------------------------------------------------

class NoStreamingClientFn[ReqT, ResR](Protocol):
    """Unary: one request → one response."""
    async def __call__(
        self, req: ReqT, metadata: Any = (), timeout: Optional[float] = None
    ) -> ResR: ...


class ServerStreamingClientFn[ReqT, ResR](Protocol):
    """Server-streaming: one request → async iterator of responses."""
    def __call__(
        self, req: ReqT, metadata: Any = (), timeout: Optional[float] = None
    ) -> ServerStreamingGRPCStream[ResR]: ...


class ClientStreamingClientFn[ReqT, ResR](Protocol):
    """Client-streaming: open a stream, send N requests, receive one response."""
    async def __call__(
        self, metadata: Any = (), timeout: Optional[float] = None
    ) -> ClientStreamingGRPCStream[ReqT, ResR]: ...


class BidiStreamingClientFn[ReqT, ResR](Protocol):
    """Bidi-streaming: open a stream, send N requests, receive M responses."""
    async def __call__(
        self, metadata: Any = (), timeout: Optional[float] = None
    ) -> BidiStreamingGRPCStream[ReqT, ResR]: ...


def _request_timeout() -> Optional[float]:
    deadline = request_deadline.get()
    if deadline is None:
        return None
    now = datetime.now(timezone.utc if deadline.tzinfo is not None else None)
    return max(0.0, (deadline - now).total_seconds())


# ---------------------------------------------------------------------------
# Sender / ResultContext — public interfaces passed to EndpointHandler.
# ---------------------------------------------------------------------------

class Sender[ReqT](Protocol):
    """Sends a gRPC request. Port of Go's datasink/grpc Sender."""
    async def send(self, req: ReqT) -> None: ...


class ResultContext(Protocol):
    """Signals end of request stream. Port of Go's datasink/grpc ResultContext."""
    def done(self) -> None: ...


class _NopResultContext:
    __slots__ = ()

    def done(self) -> None:
        pass


_NOP_RESULT_CONTEXT = _NopResultContext()


class _DoneResultContext:
    __slots__ = ("_future",)

    _future: asyncio.Future[None]

    def __init__(self):
        self._future = asyncio.get_running_loop().create_future()

    def done(self) -> None:
        if not self._future.done():
            self._future.set_result(None)

    async def wait(self) -> None:
        await self._future


# ---------------------------------------------------------------------------
# EndpointHandler
# ---------------------------------------------------------------------------

class EndpointHandler[HandlerState, ReqT, ResR, T, R, E](Protocol):
    """
    User-supplied handler for gRPC sink calls.

    Lifecycle (unary/server-streaming — one call per Consume):
        begin_request → consume_message → [gRPC call] → handle_response(N) → end_request

    Lifecycle (bidi/client-streaming — one stream per streamID):
        begin_request → consume_message(N) → handle_response(M) → end_request

    Port of Go's datasink/grpc EndpointHandler.
    """

    async def begin_request(
        self,
        sc: SinkStreamContext[T, R, E],
    ) -> HandlerState: ...

    async def consume_message(
        self,
        sc: SinkStreamContext[T, R, E],
        handler_state: HandlerState,
        value: T,
        sender: Sender[ReqT],
        result_ctx: ResultContext,
    ) -> None: ...

    async def handle_response(
        self,
        sc: SinkStreamContext[T, R, E],
        handler_state: HandlerState,
        response: ResR,
    ) -> None: ...

    async def end_request(
        self,
        sc: SinkStreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


# ---------------------------------------------------------------------------
# Senders
# ---------------------------------------------------------------------------

class _RequestSender[ReqT](Sender[ReqT]):
    """Stores the request for later use (unary / server-streaming)."""
    __slots__ = ("req",)

    req: Optional[ReqT]

    def __init__(self):
        self.req = None

    async def send(self, req: ReqT) -> None:
        self.req = req


class _GrpcStreamSender[ReqT](Sender[ReqT]):
    """Forwards send() to the live gRPC stream and records span events.
    Port of Go's grpcSender."""
    __slots__ = ("_write_fn", "_lock", "_active", "_span")

    _write_fn: Any
    _lock: asyncio.Lock
    _active: bool
    _span: Optional[Span]

    def __init__(self, write_fn: Any, span: Optional[Span] = None):
        self._write_fn = write_fn
        self._lock = asyncio.Lock()
        self._active = True
        self._span = span

    async def send(self, req: ReqT) -> None:
        async with self._lock:
            if not self._active:
                raise RuntimeError("gRPC stream is closed")
            try:
                await self._write_fn(req)
                span_event(self._span, "send")
            except Exception as e:
                span_error(self._span, e)
                span_event(self._span, "send.error", string_attr("error", str(e)))
                raise

    def close(self) -> None:
        self._active = False


# ---------------------------------------------------------------------------
# Per-stream_id session tracking for client-streaming / bidi-streaming sinks.
#
# Port of Go's clientStreamingResult/bidiStreamingResult + pending
# RotatingMap: a single gRPC stream is opened the first time Consume is
# called for a given stream_id, and reused by every subsequent Consume call
# that shares the same stream_id, until the handler signals done().
# ---------------------------------------------------------------------------

class _PendingCell[V]:
    """Reserves a pending-map slot for a stream_id before the corresponding
    session can be built (building it requires begin_request and the actual
    gRPC call to be established, both of which await). value stays None
    until ready is set, so it is never observable in a partially-constructed
    state: a concurrent consume() for the same still-being-created stream_id
    awaits ready and only ever sees a fully built session (or the recorded
    error).

    Unlike Go/Rust/C++, no lock is needed anywhere here:
    RotatingMap.get_or_create is synchronous (no await), so Python's
    single-threaded, cooperative event loop already makes the reservation
    atomic at no extra cost.
    """
    __slots__ = ("ready", "value", "error")

    def __init__(self):
        self.ready = asyncio.Event()
        self.value: Optional[V] = None
        self.error: Optional[Exception] = None


def _drop_reservation(pending: "RotatingMap[str, _PendingCell]", stream_id: str, cell: _PendingCell) -> None:
    """Remove a failed reservation so a later Consume for the same stream_id
    can retry from scratch. No-op if the entry was already replaced/removed."""
    current, found = pending.get(stream_id)
    if found and current is cell:
        pending.pop(stream_id)


class _ClientStreamingSession[HandlerState, ReqT]:
    """Tracks one open client-streaming gRPC call shared by every Consume
    for the same stream_id. Port of Go's clientStreamingResult."""
    __slots__ = ("handler_state", "grpc_stream", "span", "sender", "done_ctx",
                 "lifetime", "finished")

    def __init__(self, handler_state, grpc_stream, span, sender, done_ctx):
        self.handler_state = handler_state
        self.grpc_stream = grpc_stream
        self.span = span
        self.sender = sender
        self.done_ctx = done_ctx
        self.lifetime = AsyncRWLock()
        self.finished = False


class _BidiStreamingSession[HandlerState, ReqT]:
    """Tracks one open bidi-streaming gRPC call shared by every Consume for
    the same stream_id. Port of Go's bidiStreamingResult."""
    __slots__ = ("handler_state", "grpc_stream", "span", "sender", "done_ctx",
                 "lifetime", "finished", "recv_task")

    def __init__(self, handler_state, grpc_stream, span, sender, done_ctx):
        self.handler_state = handler_state
        self.grpc_stream = grpc_stream
        self.span = span
        self.sender = sender
        self.done_ctx = done_ctx
        self.lifetime = AsyncRWLock()
        self.finished = False
        self.recv_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Data sink / endpoint infrastructure
# ---------------------------------------------------------------------------

class _GrpcSinkDataSink(OutputDataSink):
    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast("_GrpcSinkEndpoint", ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_coros = [
            cast("_GrpcSinkEndpoint", ep).stop(ctx)
            for ep in self.endpoints
        ]
        if stop_coros:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_coros, return_exceptions=True),
                    timeout=ctx.time_left,
                )
            except (asyncio.TimeoutError, TypeError):
                self.environment.log.warn(
                    f"gRPC sink data sink '{self.name}' stopped by timeout."
                )


class _GrpcSinkEndpoint(DataSinkEndpoint):
    _consumer_obj: Optional["_GrpcSinkEndpointConsumer"]

    def __init__(self, data_sink: _GrpcSinkDataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)
        self._consumer_obj = None

    async def start(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.stop(ctx)


class _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](Consumer[T], OutputEndpointConsumer):
    _endpoint: _GrpcSinkEndpoint
    _stream: TypedSinkStreamWithResult[T, R, E]
    _handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
    _sc: SinkStreamContext[T, R, E]
    _tracing: Optional[Tracing]
    _tracer: Optional[Tracer]

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing],
        tracer: Optional[Tracer],
    ):
        self._endpoint = endpoint
        self._stream = stream
        self._handler = handler
        self._tracing = tracing
        self._tracer = tracer

        self._sc = SinkStreamContext[T, R, E](
            stream=stream,
            collect=CollectFunc[R](stream.consume_result),
            error_collect=CollectFunc[E](stream.error_stream.consume),
        )

        stream.set_sink_consumer(self)
        endpoint._consumer_obj = self
        endpoint.add_endpoint_consumer(self)

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

    async def start(self, ctx: Context) -> None:
        pass

    async def stop(self, ctx: Context) -> None:
        pass

    async def _begin(self) -> HandlerState:
        return await self._handler.begin_request(self._sc)

    async def _end(self, err: Optional[Exception], handler_state: HandlerState) -> None:
        await self._handler.end_request(self._sc, err, handler_state)

    def _metadata(self) -> list[tuple[str, str]]:
        sid = stream_id_from_context()
        if not sampling_enabled():
            return [('x-stream-id', sid)] if sid else []
        carrier: dict[str, str] = {}
        if self._tracing is not None:
            self._tracing.inject(carrier)
        carrier['x-trace'] = '1'
        if sid:
            carrier['x-stream-id'] = sid
        return list(carrier.items())


# ---------------------------------------------------------------------------
# Concrete consumers — one per gRPC streaming mode
# ---------------------------------------------------------------------------

class _NoStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):
    _client_fn: NoStreamingClientFn[ReqT, ResR]

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing],
        tracer: Optional[Tracer],
        client_fn: NoStreamingClientFn[ReqT, ResR],
    ):
        super().__init__(endpoint, stream, handler, tracing, tracer)
        self._client_fn = client_fn

    async def consume(self, value: T) -> None:
        _, span = start_endpoint_span(
            self._tracer, "grpc.output", self._stream.name, self._endpoint.name,
        )
        ep = self._endpoint
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                try:
                    handler_state = await self._begin()
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    return
                span_event(span, "begin_request")
                sender = _RequestSender[ReqT]()
                try:
                    await self._handler.consume_message(
                        self._sc, handler_state, value, sender, _NOP_RESULT_CONTEXT
                    )
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    await self._end(err, handler_state)
                    return
                span_event(span, "consume_message")

                if sender.req is None:
                    e = ValueError("no gRPC request set by handler")
                    span_error(span, e)
                    end_err = e
                    await self._end(e, handler_state)
                    return

                stream_id_token = request_stream_id.set(new_stream_id())
                try:
                    metadata = self._metadata()
                    resp = await self._client_fn(
                        sender.req, metadata=metadata, timeout=_request_timeout()
                    )
                except Exception as e:
                    span_error(span, e)
                    span_event(span, "grpc_call.error", string_attr("error", str(e)))
                    end_err = e
                    await self._end(e, handler_state)
                    return
                finally:
                    request_stream_id.reset(stream_id_token)
                span_event(span, "grpc_call")

                try:
                    await self._handler.handle_response(self._sc, handler_state, resp)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "handle_response.error", string_attr("error", str(err)))
                    end_err = err
                    await self._end(err, handler_state)
                    return

                span_event(span, "handle_response")
                await self._end(None, handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()


class _ServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):
    _client_fn: ServerStreamingClientFn[ReqT, ResR]

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing],
        tracer: Optional[Tracer],
        client_fn: ServerStreamingClientFn[ReqT, ResR],
    ):
        super().__init__(endpoint, stream, handler, tracing, tracer)
        self._client_fn = client_fn

    async def consume(self, value: T) -> None:
        _, span = start_endpoint_span(
            self._tracer, "grpc.output", self._stream.name, self._endpoint.name,
        )
        ep = self._endpoint
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                try:
                    handler_state = await self._begin()
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    return
                span_event(span, "begin_request")
                sender = _RequestSender[ReqT]()
                try:
                    await self._handler.consume_message(
                        self._sc, handler_state, value, sender, _NOP_RESULT_CONTEXT
                    )
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    await self._end(err, handler_state)
                    return
                span_event(span, "consume_message")

                if sender.req is None:
                    e = ValueError("no gRPC request set by handler")
                    span_error(span, e)
                    end_err = e
                    await self._end(e, handler_state)
                    return

                stream_id_token = request_stream_id.set(new_stream_id())
                try:
                    metadata = self._metadata()
                    grpc_stream = self._client_fn(
                        sender.req, metadata=metadata, timeout=_request_timeout()
                    )
                except Exception as e:
                    span_error(span, e)
                    span_event(span, "grpc_call.error", string_attr("error", str(e)))
                    end_err = e
                    await self._end(e, handler_state)
                    return
                finally:
                    request_stream_id.reset(stream_id_token)
                span_event(span, "grpc_call")

                async for resp in grpc_stream:
                    try:
                        await self._handler.handle_response(self._sc, handler_state, resp)
                    except Exception as err:
                        span_error(span, err)
                        span_event(span, "handle_response.error", string_attr("error", str(err)))
                        end_err = err
                        await self._end(err, handler_state)
                        return

                span_event(span, "eof")
                await self._end(None, handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()


class _ClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):
    _client_fn: ClientStreamingClientFn[ReqT, ResR]
    _pending: "RotatingMap[str, _PendingCell[_ClientStreamingSession]]"

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing],
        tracer: Optional[Tracer],
        client_fn: ClientStreamingClientFn[ReqT, ResR],
    ):
        super().__init__(endpoint, stream, handler, tracing, tracer)
        self._client_fn = client_fn
        self._pending = RotatingMap(interval=30.0)

    async def start(self, ctx: Context) -> None:
        await self._pending.start(ctx)

    async def stop(self, ctx: Context) -> None:
        await self._pending.stop(ctx)

    async def consume(self, value: T) -> None:
        stream_id = stream_id_from_context()
        if stream_id is None:
            stream_id = with_stream_id(new_stream_id())

        cell, loaded = self._pending.get_or_create(
            stream_id, lambda: _PendingCell[_ClientStreamingSession]()
        )
        ep = self._endpoint
        session: _ClientStreamingSession

        if not loaded:
            _, span = start_endpoint_span(
                self._tracer, "grpc.output", self._stream.name, self._endpoint.name,
            )
            with span.scoped():
                try:
                    handler_state = await self._begin()
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    span.end()
                    cell.error = err
                    cell.ready.set()
                    _drop_reservation(self._pending, stream_id, cell)
                    return
                span_event(span, "begin_request")
                stream_id_token = request_stream_id.set(new_stream_id())
                start_time = ep.on_request_start()

                try:
                    try:
                        metadata = self._metadata()
                        grpc_stream = await self._client_fn(
                            metadata=metadata, timeout=_request_timeout()
                        )
                    finally:
                        request_stream_id.reset(stream_id_token)
                except Exception as e:
                    span_error(span, e)
                    span_event(span, "grpc_call.error", string_attr("error", str(e)))
                    await self._end(e, handler_state)
                    ep.on_request_end(start_time, e)
                    span.end()
                    cell.error = e
                    cell.ready.set()
                    _drop_reservation(self._pending, stream_id, cell)
                    return
                span_event(span, "grpc_call")

                done_ctx = _DoneResultContext()
                sender = _GrpcStreamSender[ReqT](grpc_stream.write, span)
                session = _ClientStreamingSession(
                    handler_state, grpc_stream, span, sender, done_ctx
                )
                cell.value = session
                cell.ready.set()
                asyncio.create_task(
                    self._complete(stream_id, cell, session, start_time)
                )
        else:
            await cell.ready.wait()
            if cell.error is not None:
                # Creation failed on another task; it has already reported
                # and cleaned up, so this message is simply dropped.
                return
            assert cell.value is not None
            session = cell.value

        async with session.lifetime.read_lock():
            if session.finished:
                return
            current, still_pending = self._pending.get(stream_id)
            if not still_pending or current is not cell:
                ep.on_late_result(stream_id)
                return
            with session.span.scoped():
                try:
                    await self._handler.consume_message(
                        self._sc, session.handler_state, value, session.sender,
                        session.done_ctx,
                    )
                    span_event(session.span, "consume_message")
                except Exception as err:
                    span_error(session.span, err)
                    span_event(
                        session.span, "consume_message.error",
                        string_attr("error", str(err)),
                    )
                    session.done_ctx.done()

    async def _complete(
        self,
        stream_id: str,
        cell: "_PendingCell[_ClientStreamingSession]",
        session: "_ClientStreamingSession",
        start_time: float,
    ) -> None:
        ep = self._endpoint
        span = session.span
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                await session.done_ctx.wait()
                async with session.lifetime.write_lock():
                    session.finished = True
                    session.sender.close()
                    current, found = self._pending.get(stream_id)
                    if found and current is cell:
                        self._pending.pop(stream_id)
                    await session.grpc_stream.done_writing()

                    try:
                        resp = await session.grpc_stream  # type: ignore[misc]
                    except Exception as e:
                        span_error(span, e)
                        span_event(span, "close_and_recv.error", string_attr("error", str(e)))
                        end_err = e
                        await self._end(e, session.handler_state)
                        return

                    try:
                        await self._handler.handle_response(
                            self._sc, session.handler_state, resp
                        )
                    except Exception as err:
                        span_error(span, err)
                        span_event(span, "handle_response.error", string_attr("error", str(err)))
                        end_err = err
                        await self._end(err, session.handler_state)
                        return

                    span_event(span, "handle_response")
                    await self._end(None, session.handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()


class _BidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):
    _client_fn: BidiStreamingClientFn[ReqT, ResR]
    _pending: "RotatingMap[str, _PendingCell[_BidiStreamingSession]]"

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing],
        tracer: Optional[Tracer],
        client_fn: BidiStreamingClientFn[ReqT, ResR],
    ):
        super().__init__(endpoint, stream, handler, tracing, tracer)
        self._client_fn = client_fn
        self._pending = RotatingMap(interval=30.0)

    async def start(self, ctx: Context) -> None:
        await self._pending.start(ctx)

    async def stop(self, ctx: Context) -> None:
        await self._pending.stop(ctx)

    async def consume(self, value: T) -> None:
        stream_id = stream_id_from_context()
        if stream_id is None:
            stream_id = with_stream_id(new_stream_id())

        cell, loaded = self._pending.get_or_create(
            stream_id, lambda: _PendingCell[_BidiStreamingSession]()
        )
        ep = self._endpoint
        session: _BidiStreamingSession

        if not loaded:
            _, span = start_endpoint_span(
                self._tracer, "grpc.output", self._stream.name, self._endpoint.name,
            )
            with span.scoped():
                try:
                    handler_state = await self._begin()
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    span.end()
                    cell.error = err
                    cell.ready.set()
                    _drop_reservation(self._pending, stream_id, cell)
                    return
                span_event(span, "begin_request")
                stream_id_token = request_stream_id.set(new_stream_id())
                start_time = ep.on_request_start()

                try:
                    try:
                        metadata = self._metadata()
                        grpc_stream = await self._client_fn(
                            metadata=metadata, timeout=_request_timeout()
                        )
                    finally:
                        request_stream_id.reset(stream_id_token)
                except Exception as e:
                    span_error(span, e)
                    span_event(span, "grpc_call.error", string_attr("error", str(e)))
                    await self._end(e, handler_state)
                    ep.on_request_end(start_time, e)
                    span.end()
                    cell.error = e
                    cell.ready.set()
                    _drop_reservation(self._pending, stream_id, cell)
                    return
                span_event(span, "grpc_call")

                done_ctx = _DoneResultContext()
                sender = _GrpcStreamSender[ReqT](grpc_stream.write, span)
                session = _BidiStreamingSession(
                    handler_state, grpc_stream, span, sender, done_ctx
                )
                session.recv_task = asyncio.create_task(
                    self._recv_loop(session)
                )
                cell.value = session
                cell.ready.set()
                asyncio.create_task(
                    self._complete(stream_id, cell, session, start_time)
                )
        else:
            await cell.ready.wait()
            if cell.error is not None:
                # Creation failed on another task; it has already reported
                # and cleaned up, so this message is simply dropped.
                return
            assert cell.value is not None
            session = cell.value

        async with session.lifetime.read_lock():
            if session.finished:
                return
            current, still_pending = self._pending.get(stream_id)
            if not still_pending or current is not cell:
                ep.on_late_result(stream_id)
                return
            with session.span.scoped():
                try:
                    await self._handler.consume_message(
                        self._sc, session.handler_state, value, session.sender,
                        session.done_ctx,
                    )
                    span_event(session.span, "consume_message")
                except Exception as err:
                    span_error(session.span, err)
                    span_event(
                        session.span, "consume_message.error",
                        string_attr("error", str(err)),
                    )
                    session.done_ctx.done()

    async def _recv_loop(self, session: "_BidiStreamingSession") -> Optional[Exception]:
        span = session.span
        try:
            async for resp in session.grpc_stream:
                try:
                    await self._handler.handle_response(
                        self._sc, session.handler_state, resp
                    )
                except Exception as e:
                    span_error(span, e)
                    span_event(span, "handle_response.error", string_attr("error", str(e)))
                    return e
        except Exception as e:
            span_error(span, e)
            span_event(span, "recv.error", string_attr("error", str(e)))
            return e
        return None

    async def _complete(
        self,
        stream_id: str,
        cell: "_PendingCell[_BidiStreamingSession]",
        session: "_BidiStreamingSession",
        start_time: float,
    ) -> None:
        ep = self._endpoint
        span = session.span
        recv_err: Optional[Exception] = None
        try:
            with span.scoped():
                await session.done_ctx.wait()
                async with session.lifetime.write_lock():
                    session.finished = True
                    session.sender.close()
                    await session.grpc_stream.done_writing()
                    assert session.recv_task is not None
                    recv_err = await session.recv_task

                    current, found = self._pending.get(stream_id)
                    if found and current is cell:
                        self._pending.pop(stream_id)

                    if recv_err is not None:
                        span_error(span, recv_err)
                    await self._end(recv_err, session.handler_state)
        finally:
            ep.on_request_end(start_time, recv_err)
            span.end()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_or_create_datasink(
    id_endpoint: int,
    env: ServiceExecutionEnvironment,
) -> "_GrpcSinkDataSink":
    cfg_ep = env.config.get_endpoint_config_by_id(id_endpoint)
    datasink = env.get_datasink(cfg_ep.id_data_connector)
    if datasink is not None:
        return cast(_GrpcSinkDataSink, datasink)
    cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
    ds = _GrpcSinkDataSink(connector_id=cfg_ds.id, env=env)
    env.add_datasink(ds)
    return ds


def _get_or_create_endpoint(
    stream: TypedSinkStreamWithResult,
    ds: _GrpcSinkDataSink,
) -> "_GrpcSinkEndpoint":
    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is not None:
        return cast(_GrpcSinkEndpoint, endpoint)
    ep = _GrpcSinkEndpoint(data_sink=ds, id_endpoint=stream.endpoint_id)
    ds.add_endpoint(ep)
    return ep


def _make_tracer(stream: TypedSinkStreamWithResult) -> Optional[Tracer]:
    tracing = stream.environment.tracing
    if tracing is None:
        return None
    return tracing.tracer(stream.environment.service_config.name)


# ---------------------------------------------------------------------------
# Factory functions — one per gRPC streaming mode
# ---------------------------------------------------------------------------

def make_grpc_no_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: "NoStreamingClientFn[ReqT, ResR]",
) -> Consumer[T]:
    """Unary gRPC sink: one request → one response per Consume."""
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _NoStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, _make_tracer(stream), client_fn
    )


def make_grpc_server_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: "ServerStreamingClientFn[ReqT, ResR]",
) -> Consumer[T]:
    """Server-streaming gRPC sink: one request → N responses per Consume."""
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _ServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, _make_tracer(stream), client_fn
    )


def make_grpc_client_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: "ClientStreamingClientFn[ReqT, ResR]",
) -> Consumer[T]:
    """Client-streaming gRPC sink: N requests → one response per Consume."""
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _ClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, _make_tracer(stream), client_fn
    )


def make_grpc_bidi_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: "BidiStreamingClientFn[ReqT, ResR]",
) -> Consumer[T]:
    """Bidi-streaming gRPC sink: N requests → M responses per Consume."""
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _BidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, _make_tracer(stream), client_fn
    )

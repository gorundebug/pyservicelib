#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

"""
gRPC datasource endpoint consumers for all four streaming modes.

Equivalent to Go's datasource/grpc/ package (grpc.go + nostreaming.go +
serverstreaming.go + clientstreaming.go + bidistreaming.go).

In Python/grpcio.aio, gRPC method implementations are coroutines passed to the
server. Each make_* function returns:
  - A Consumer[T] that the stream uses to receive pipeline results.
  - An async handler coroutine whose signature matches the generated servicer method.

The user passes the returned handler directly as the gRPC servicer method.
"""

import asyncio
from abc import abstractmethod, ABC
from collections.abc import Coroutine
from typing import Optional, Protocol, Any, AsyncIterator, Callable, cast

import grpc
import grpc.aio

from ...runtime.environment.tracing import (
    Tracer, Tracing, Span, start_span, span_event, span_error, span_attrs,
    string_attr, bool_attr, sampling_enabled, sampling_scope,
)
from ...runtime.common import (
    TypedInputStream, InputEndpoint, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap

_PENDING_ROTATION_INTERVAL = 30.0  # seconds


def _stream_id_from_grpc_metadata(context: grpc.aio.ServicerContext[Any, Any]) -> Optional[str]:
    return _grpc_metadata(context).get('x-stream-id')


def _grpc_metadata(context: grpc.aio.ServicerContext[Any, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():  # type: ignore[union-attr]
        decoded_key = key.decode('utf-8') if isinstance(key, bytes) else key
        decoded_value = value.decode('utf-8') if isinstance(value, bytes) else value
        result[decoded_key.lower()] = decoded_value
    return result


# ---------------------------------------------------------------------------
# Handler type aliases (one per gRPC streaming mode)
# ---------------------------------------------------------------------------

type UnaryHandler[ReqT, ResR] = Callable[
    [ReqT, grpc.aio.ServicerContext[ReqT, ResR]],
    Coroutine[Any, Any, ResR],
]
type ServerStreamingHandler[ReqT, ResR] = Callable[
    [ReqT, grpc.aio.ServicerContext[ReqT, ResR]],
    Coroutine[Any, Any, None],
]
type ClientStreamingHandler[ReqT, ResR] = Callable[
    [AsyncIterator[ReqT], grpc.aio.ServicerContext[ReqT, ResR]],
    Coroutine[Any, Any, ResR],
]
type BidiStreamingHandler[ReqT, ResR] = Callable[
    [AsyncIterator[ReqT], grpc.aio.ServicerContext[ReqT, ResR]],
    Coroutine[Any, Any, None],
]


# ---------------------------------------------------------------------------
# Sender Protocol and implementations
# ---------------------------------------------------------------------------

class Sender[ResR](Protocol):
    """Sends a result value back to the gRPC client. Equivalent to Go's Sender[R, ResR]."""
    async def send(self, value: ResR) -> None: ...


class ResultCallback[HandlerState, T, ResR, R, E](Protocol):
    """Return True to deregister; False to keep active. Equivalent to Go's ResultCallback."""
    def __call__(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
        sender: Sender[ResR],
    ) -> bool: ...


class ResultContext[HandlerState, T, ResR, R, E](ABC):
    """
    Allows the handler to register result callbacks and signal completion.
    Equivalent to Go's datasource/grpc ResultContext.
    """

    @abstractmethod
    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, ResR, R, E]",
    ) -> None: ...

    @abstractmethod
    def done(self) -> None: ...


class _NoopResultContext[HandlerState, T, ResR, R, E](ResultContext[HandlerState, T, ResR, R, E]):
    def set_result_callback(self, message_id: str, cb: Any) -> None:
        pass

    def done(self) -> None:
        pass


class _GrpcResult[HandlerState, T, ResR, R, E](ResultContext[HandlerState, T, ResR, R, E]):
    handler_state: HandlerState
    sender: Sender[ResR]
    _done: asyncio.Event
    _callbacks: dict[str, Any]
    _cb_lock: asyncio.Lock
    _span: Optional[Span]
    _once: bool  # guards done() to fire span event exactly once

    def __init__(self, handler_state: HandlerState, sender: Sender[ResR], span: Optional[Span] = None):
        self.handler_state = handler_state
        self.sender = sender
        self._done = asyncio.Event()
        self._callbacks = {}
        self._cb_lock = asyncio.Lock()
        self._span = span
        self._once = False

    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, ResR, R, E]",
    ) -> None:
        self._callbacks[message_id] = cb

    def done(self) -> None:
        if not self._once:
            self._once = True
            span_event(self._span, "done_called")
        self._done.set()


class EndpointHandler[HandlerState, ReqT, ResR, T, R, E](Protocol):
    """
    User-supplied handler for gRPC source calls.

    Lifecycle (unary, server-streaming):
        begin_request → consume_message → eof → [await result] → end_request

    Lifecycle (client-streaming, bidi-streaming):
        begin_request → consume_message (N times) → eof → [await result] → end_request

    Equivalent to Go's datasource/grpc EndpointHandler.
    """

    async def begin_request(
        self,
        sc: StreamContext[T, R, E],
    ) -> HandlerState: ...

    async def consume_message(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        req: ReqT,
        result_ctx: "ResultContext[HandlerState, T, ResR, R, E]",
        sender: Sender[ResR],
    ) -> None: ...

    def get_message_id(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
    ) -> str: ...

    def eof(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
    ) -> None: ...

    async def end_request(
        self,
        sc: StreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


# ---------------------------------------------------------------------------
# DataSource / Endpoint wrappers
# ---------------------------------------------------------------------------

class _GrpcDataSource(InputDataSource):
    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast("_GrpcEndpoint", ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast("_GrpcEndpoint", ep).stop(ctx)


class _GrpcEndpoint(DataSourceEndpoint):
    _consumer_obj: Optional["_GrpcTypedEndpointConsumer"]

    def __init__(self, datasource: _GrpcDataSource, id_endpoint: int):
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        self._consumer_obj = None

    async def start(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer_obj is not None:
            await self._consumer_obj.stop(ctx)


# ---------------------------------------------------------------------------
# Sender implementations
# ---------------------------------------------------------------------------

class _UnarySender[ResR](Sender[ResR]):
    _future: "asyncio.Future[ResR]"
    _span: Optional[Span]

    def __init__(self, span: Optional[Span] = None):
        self._future: asyncio.Future[ResR] = asyncio.get_event_loop().create_future()
        self._span = span

    async def send(self, value: ResR) -> None:
        if not self._future.done():
            self._future.set_result(value)
            span_event(self._span, "send")
        else:
            err = RuntimeError("result already sent")
            span_error(self._span, err)
            span_event(self._span, "send.error", string_attr("error", str(err)))
            raise err


class _StreamSender[ResR](Sender[ResR]):
    _send_fn: Any  # grpc.aio.ServicerContext.write or stream.send
    _lock: asyncio.Lock
    _active: bool
    _span: Optional[Span]

    def __init__(self, send_fn: Any, span: Optional[Span] = None):
        self._send_fn = send_fn
        self._lock = asyncio.Lock()
        self._active = True
        self._span = span

    async def send(self, value: ResR) -> None:
        async with self._lock:
            if not self._active:
                err = grpc.RpcError("stream is closed")
                span_error(self._span, err)
                span_event(self._span, "send.error", string_attr("error", "stream is closed"))
                raise err
            try:
                await self._send_fn(value)
                span_event(self._span, "send")
            except Exception as e:
                span_error(self._span, e)
                span_event(self._span, "send.error", string_attr("error", str(e)))
                raise

    def close(self) -> None:
        self._active = False


# ---------------------------------------------------------------------------
# Result consumer proxy
# ---------------------------------------------------------------------------

class _ResultConsumerProxy[R](Consumer[R]):
    def __init__(self, consumer: "_GrpcTypedEndpointConsumer") -> None:  # type: ignore[type-arg]
        self._consumer = consumer

    async def consume(self, value: R) -> None:
        await self._consumer._consume_result(value)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Core endpoint consumer
# ---------------------------------------------------------------------------

def _make_tracer(stream: TypedInputStream, env: ServiceExecutionEnvironment) -> Optional[Tracer]:
    tracing = env.tracing
    if tracing is None:
        return None
    return tracing.tracer(env.service_config.name)


class _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](DataSourceEndpointConsumer[T, R, E]):
    _handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E]
    _sc: StreamContext[T, R, E]
    _has_result: bool
    _pending: Optional[RotatingMap[str, _GrpcResult[HandlerState, T, ResR, R, E]]]
    _tracing: Optional[Tracing]
    _tracer: Optional[Tracer]

    def __init__(
        self,
        endpoint: _GrpcEndpoint,
        stream: TypedInputStream[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        tracing: Optional[Tracing] = None,
        tracer: Optional[Tracer] = None,
    ):
        super().__init__(endpoint=endpoint, input_stream=stream)
        self._handler = handler
        self._has_result = stream.get_result_stream() is not None
        self._pending = None
        self._tracing = tracing
        self._tracer = tracer

        self._sc = StreamContext[T, R, E](
            stream=stream,
            result_stream=stream.get_result_stream(),
            collect=CollectFunc[T](stream.consume),
            error_collect=CollectFunc[E](stream.error_stream.consume),
        )

        if self._has_result:
            stream.set_result_consumer(_ResultConsumerProxy[R](self))  # type: ignore[arg-type]

        endpoint._consumer_obj = self
        endpoint.add_endpoint_consumer(self)

    async def start(self, ctx: Context) -> None:
        if self._has_result:
            self._pending = RotatingMap[str, Any](_PENDING_ROTATION_INTERVAL)
            await self._pending.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._pending is not None:
            await self._pending.stop(ctx)

    async def _consume_result(self, value: R) -> None:
        if not self._has_result or self._pending is None:
            return
        sid = stream_id_from_context()
        ep: InputEndpoint = self._endpoint
        if sid is None:
            ep.on_missing_stream_id()
            return
        result, found = self._pending.get(sid)
        if not found or result is None:
            ep.on_late_result(sid)
            return

        message_id = self._handler.get_message_id(self._sc, result.handler_state, value)

        async with result._cb_lock:
            cb = result._callbacks.get(message_id)
            if cb is None:
                ep.on_unknown_message_id(sid, message_id)
                span_event(result._span, "unknown_message_id", string_attr("message_id", message_id))
                return
            remove = cb(self._sc, result.handler_state, value, result.sender)
            if remove:
                if result._callbacks.pop(message_id, None) is None:
                    ep.on_duplicate_message_id(sid, message_id)
                    span_event(result._span, "duplicate_message_id", string_attr("message_id", message_id))

        span_event(result._span, "result_consumed", string_attr("message_id", message_id))

    async def _handle_common(
        self,
        grpc_context: grpc.aio.ServicerContext[Any, Any],
        sid: str,
        req: ReqT,
        sender: "Sender[ResR]",
        eof_after_first: bool,
        request_iter: Optional[AsyncIterator[ReqT]] = None,
    ) -> Optional[Exception]:
        carrier = _grpc_metadata(grpc_context)
        if self._tracing is None:
            with sampling_scope(bool(carrier.get('x-trace'))):
                return await self._handle_common_inner(
                    sid, req, sender, eof_after_first, request_iter
                )
        with self._tracing.extract(carrier) as remote_sampled:
            with sampling_scope(bool(carrier.get('x-trace')) or remote_sampled):
                return await self._handle_common_inner(
                    sid, req, sender, eof_after_first, request_iter
                )

    async def _handle_common_inner(
        self,
        sid: str,
        req: ReqT,
        sender: "Sender[ResR]",
        eof_after_first: bool,
        request_iter: Optional[AsyncIterator[ReqT]] = None,
    ) -> Optional[Exception]:
        """Shared request lifecycle used by all streaming modes."""
        with_stream_id(sid)

        _, span = start_span(
            self._tracer if sampling_enabled() else None,
            "grpc.input",
            string_attr("stream", self._sc.stream.name),
            string_attr("endpoint", self._endpoint.name),
        )
        span_scope = span.scoped()
        span_scope.__enter__()

        try:
            try:
                handler_state = await self._handler.begin_request(self._sc)
            except Exception as err:
                span_error(span, err)
                span_event(span, "begin_request.error", string_attr("error", str(err)))
                return err

            span_event(span, "begin_request")
            ep: InputEndpoint = self._endpoint
            start_time = ep.on_request_start()

            # Propagate span to sender
            if isinstance(sender, (_UnarySender, _StreamSender)):
                sender._span = span

            span_attrs(span,
                string_attr("stream_id", sid),
                bool_attr("has_result", self._has_result),
            )

            result: Optional[_GrpcResult[HandlerState, T, ResR, R, E]] = None
            result_ctx: ResultContext[HandlerState, T, ResR, R, E]

            if self._has_result and self._pending is not None:
                result = _GrpcResult(handler_state, sender, span)
                self._pending.set(sid, result)
                ep.on_pending_add(sid)
                result_ctx = result
            else:
                result_ctx = _NoopResultContext()

            # Process request(s)
            if request_iter is not None:
                # Client/bidi streaming: iterate all incoming messages
                async for r in request_iter:
                    try:
                        await self._handler.consume_message(
                            self._sc, handler_state, r, result_ctx, sender
                        )
                    except Exception as err:
                        span_error(span, err)
                        span_event(span, "consume_message.error", string_attr("error", str(err)))
                        if result is not None and self._pending is not None:
                            self._pending.pop(sid)
                            ep.on_pending_remove(sid)
                        self._handler.eof(self._sc, handler_state)
                        try:
                            await self._handler.end_request(self._sc, err, handler_state)
                        except Exception as end_err:
                            span_error(span, end_err)
                            ep.on_request_end(start_time, end_err)
                            return end_err
                        ep.on_request_end(start_time, err)
                        return err
            else:
                # Unary/server streaming: single request
                try:
                    await self._handler.consume_message(
                        self._sc, handler_state, req, result_ctx, sender
                    )
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    if result is not None and self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                    self._handler.eof(self._sc, handler_state)
                    try:
                        await self._handler.end_request(self._sc, err, handler_state)
                    except Exception as end_err:
                        span_error(span, end_err)
                        ep.on_request_end(start_time, end_err)
                        return end_err
                    ep.on_request_end(start_time, err)
                    return err

            span_event(span, "consume_message")
            self._handler.eof(self._sc, handler_state)
            span_event(span, "eof")

            if not self._has_result or result is None:
                try:
                    await self._handler.end_request(self._sc, None, handler_state)
                except Exception as end_err:
                    span_error(span, end_err)
                    ep.on_request_end(start_time, end_err)
                    return end_err
                ep.on_request_end(start_time, None)
                return None

            try:
                await result._done.wait()
                span_event(span, "done_received")
            except asyncio.CancelledError:
                cancel_err = asyncio.CancelledError()
                span_error(span, cancel_err)  # type: ignore[arg-type]
                span_event(span, "context_cancelled", string_attr("error", "cancelled"))
            finally:
                if self._pending is not None:
                    self._pending.pop(sid)
                    ep.on_pending_remove(sid)

            try:
                await self._handler.end_request(self._sc, None, handler_state)
            except Exception as end_err:
                span_error(span, end_err)
                ep.on_request_end(start_time, end_err)
                return end_err
            ep.on_request_end(start_time, None)
            return None

        finally:
            span_scope.__exit__(None, None, None)
            span.end()


# ---------------------------------------------------------------------------
# Helpers to get or create datasource / endpoint
# ---------------------------------------------------------------------------

def _get_or_create_datasource(
    id_endpoint: int,
    env: ServiceExecutionEnvironment,
) -> "_GrpcDataSource":
    cfg_ep = env.config.get_endpoint_config_by_id(id_endpoint)
    datasource = env.get_datasource(cfg_ep.id_data_connector)
    if datasource is not None:
        return cast(_GrpcDataSource, datasource)
    cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
    ds = _GrpcDataSource(connector_id=cfg_ds.id, env=env)
    env.add_datasource(ds)
    return ds


def _get_or_create_endpoint(
    stream: TypedInputStream,
    ds: _GrpcDataSource,
) -> "_GrpcEndpoint":
    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is not None:
        return cast(_GrpcEndpoint, endpoint)
    ep = _GrpcEndpoint(datasource=ds, id_endpoint=stream.endpoint_id)
    ds.add_endpoint(ep)
    return ep


# ---------------------------------------------------------------------------
# Factory functions — one per gRPC streaming mode
# ---------------------------------------------------------------------------

def make_grpc_no_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], UnaryHandler[ReqT, ResR]]":
    """Unary gRPC source: one request → one response."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, tracer
    )

    async def _handle(request: ReqT, context: grpc.aio.ServicerContext[ReqT, ResR]) -> ResR:
        sid = _stream_id_from_grpc_metadata(context) or new_stream_id()
        sender = _UnarySender[ResR]()
        err = await ec._handle_common(context, sid, request, sender, eof_after_first=True)
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))
        return await sender._future

    return ec, _handle


def make_grpc_server_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], ServerStreamingHandler[ReqT, ResR]]":
    """Server-streaming gRPC source: one request → N responses."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, tracer
    )

    async def _handle(request: ReqT, context: grpc.aio.ServicerContext[ReqT, ResR]) -> None:
        sid = _stream_id_from_grpc_metadata(context) or new_stream_id()
        sender = _StreamSender[ResR](context.write)
        err = await ec._handle_common(context, sid, request, sender, eof_after_first=True)
        sender.close()
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))

    return ec, _handle


def make_grpc_client_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], ClientStreamingHandler[ReqT, ResR]]":
    """Client-streaming gRPC source: N requests → one response."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, tracer
    )

    async def _handle(
        request_iterator: AsyncIterator[ReqT],
        context: grpc.aio.ServicerContext[ReqT, ResR],
    ) -> ResR:
        sid = _stream_id_from_grpc_metadata(context) or new_stream_id()
        sender = _UnarySender[ResR]()
        err = await ec._handle_common(
            context, sid, cast(ReqT, None), sender, eof_after_first=False,
            request_iter=request_iterator,
        )
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))
        return await sender._future

    return ec, _handle


def make_grpc_bidi_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], BidiStreamingHandler[ReqT, ResR]]":
    """Bidi-streaming gRPC source: N requests → N responses."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, stream.environment.tracing, tracer
    )

    async def _handle(
        request_iterator: AsyncIterator[ReqT],
        context: grpc.aio.ServicerContext[ReqT, ResR],
    ) -> None:
        sid = _stream_id_from_grpc_metadata(context) or new_stream_id()
        sender = _StreamSender[ResR](context.write)
        err = await ec._handle_common(
            context, sid, cast(ReqT, None), sender, eof_after_first=False,
            request_iter=request_iterator,
        )
        sender.close()
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))

    return ec, _handle

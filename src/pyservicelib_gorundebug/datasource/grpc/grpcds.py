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
import sys
from abc import abstractmethod, ABC
from collections.abc import Awaitable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol, Any, AsyncIterator, Callable, TYPE_CHECKING, cast

import grpc
import grpc.aio

if TYPE_CHECKING:
    from grpc.aio import DoneCallback

from ...runtime.stream_grouping import stream_grouping
from ...runtime.environment.tracing import (
    sampling_enabled,
    Tracer, Tracing, Span, NOOP_SPAN, start_endpoint_span, span_error, span_attrs,
    string_attr, bool_attr, sampling_scope,
    data_source_endpoint_tracing_enabled,
)
from ...runtime.common import (
    TypedInputStream, InputEndpoint, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import (
    new_stream_id, request_stream_id, stream_id_from_context,
    request_deadline, request_cancelled,
)
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.utils.asyncrwlock import AsyncRWLock

_PENDING_ROTATION_INTERVAL = 30.0  # seconds


def _grpc_metadata(
    context: grpc.aio.ServicerContext[Any, Any], tracing_enabled: bool,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():  # type: ignore[union-attr]
        decoded_key = key.decode('utf-8') if isinstance(key, bytes) else key
        name = decoded_key.lower()
        if not tracing_enabled and name != 'x-stream-id':
            continue
        decoded_value = value.decode('utf-8') if isinstance(value, bytes) else value
        result[name] = decoded_value
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
    ) -> bool | Awaitable[bool]: ...


class ResultContext[HandlerState, T, ResR, R, E](ABC):
    """
    Allows the handler to register result callbacks and signal completion.
    Equivalent to Go's datasource/grpc ResultContext.
    """

    __slots__ = ()

    @abstractmethod
    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, ResR, R, E]",
    ) -> None: ...

    @abstractmethod
    def done(self) -> None: ...


class _NoopResultContext[HandlerState, T, ResR, R, E](ResultContext[HandlerState, T, ResR, R, E]):
    __slots__ = ()

    def set_result_callback(self, message_id: str, cb: Any) -> None:
        pass

    def done(self) -> None:
        pass


_NOOP_RESULT_CONTEXT: Any = _NoopResultContext()


class _GrpcResult[HandlerState, T, ResR, R, E](ResultContext[HandlerState, T, ResR, R, E]):
    __slots__ = ("handler_state", "sender", "_done", "_callbacks", "_span", "_once",
                 "closed", "lifetime")

    handler_state: HandlerState
    sender: Sender[ResR]
    _done: asyncio.Future[None]
    _callbacks: dict[str, Any]
    _span: Optional[Span]
    _once: bool  # guards done() to fire span event exactly once

    def __init__(self, handler_state: HandlerState, sender: Sender[ResR], span: Optional[Span] = None):
        self.handler_state = handler_state
        self.sender = sender
        self._done = asyncio.get_running_loop().create_future()
        self._callbacks = {}
        self._span = span
        self._once = False
        self.closed = False
        self.lifetime = AsyncRWLock()

    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, ResR, R, E]",
    ) -> None:
        self._callbacks[message_id] = cb

    def done(self) -> None:
        if not self._once:
            self._once = True
            if self._span is not None and self._span is not NOOP_SPAN:
                self._span.add_event("done_called")
        if not self._done.done():
            self._done.set_result(None)


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
    __slots__ = ("_future", "_span")

    _future: "asyncio.Future[ResR]"
    _span: Optional[Span]

    def __init__(self, span: Optional[Span] = None):
        self._future: asyncio.Future[ResR] = asyncio.get_event_loop().create_future()
        self._span = span

    async def send(self, value: ResR) -> None:
        if not self._future.done():
            self._future.set_result(value)
            if self._span is not None and self._span is not NOOP_SPAN:
                self._span.add_event("send")
        else:
            err = RuntimeError("result already sent")
            if self._span is not None and self._span is not NOOP_SPAN:
                span_error(self._span, err)
                self._span.add_event("send.error", string_attr("error", str(err)))
            raise err


class _ClientStreamingSender[ResR](_UnarySender[ResR]):
    __slots__ = ("_done",)

    def __init__(self) -> None:
        super().__init__()
        self._done: Optional[Callable[[], None]] = None

    async def send(self, value: ResR) -> None:
        # Go's client-streaming SendAndClose is guarded by sync.Once.
        if self._future.done():
            return
        await super().send(value)
        if self._done is not None:
            self._done()


class _StreamSender[ResR](Sender[ResR]):
    __slots__ = ("_send_fn", "_lock", "_active", "_span")

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
                if self._span is not None and self._span is not NOOP_SPAN:
                    span_error(self._span, err)
                    self._span.add_event("send.error", string_attr("error", "stream is closed"))
                raise err
            try:
                await self._send_fn(value)
                if self._span is not None and self._span is not NOOP_SPAN:
                    self._span.add_event("send")
            except Exception as e:
                if self._span is not None and self._span is not NOOP_SPAN:
                    span_error(self._span, e)
                    self._span.add_event("send.error", string_attr("error", str(e)))
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

        self._pipeline_name, self._component_name = stream_grouping(stream)
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
        # Every RPC reserves its ID, including endpoints without result paths.
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
        if not found or result is None or result.closed:
            ep.on_late_result(sid)
            return

        # Callbacks share the read side. Only terminal lifecycle takes the
        # exclusive side, so independent result callbacks may still overlap.
        async with result.lifetime.read_lock():
            current, found = self._pending.get(sid)
            if not found or current is not result or result.closed:
                ep.on_late_result(sid)
                return
            message_id = self._handler.get_message_id(self._sc, result.handler_state, value)
            cb = result._callbacks.get(message_id)
            if cb is None:
                ep.on_unknown_message_id(sid, message_id)
                if result._span is not None and result._span is not NOOP_SPAN:
                    result._span.add_event("unknown_message_id", string_attr("message_id", message_id))
                return
            remove = cb(self._sc, result.handler_state, value, result.sender)
            if isinstance(remove, Awaitable):
                remove = await remove
            if remove and result._callbacks.pop(message_id, None) is None:
                ep.on_duplicate_message_id(sid, message_id)
                if result._span is not None and result._span is not NOOP_SPAN:
                    result._span.add_event("duplicate_message_id", string_attr("message_id", message_id))
            if result._span is not None and result._span is not NOOP_SPAN:
                result._span.add_event("result_consumed", string_attr("message_id", message_id))

    async def _handle_common(
        self,
        grpc_context: grpc.aio.ServicerContext[Any, Any],
        carrier: dict[str, str],
        sid: str,
        req: ReqT,
        sender: "Sender[ResR]",
        eof_after_first: bool,
        request_iter: Optional[AsyncIterator[ReqT]] = None,
    ) -> Optional[Exception]:
        loop = asyncio.get_running_loop()
        remaining = grpc_context.time_remaining()
        deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=remaining)
            if remaining is not None else None
        )
        cancelled = asyncio.Event()
        deadline_token = request_deadline.set(deadline)
        cancelled_token = request_cancelled.set(cancelled)
        timer = None
        try:
            if remaining is not None:
                timer = loop.call_later(max(0, remaining), cancelled.set)
            def on_done(_: grpc.aio.ServicerContext[Any, Any]) -> None:
                loop.call_soon_threadsafe(cancelled.set)

            # grpc-stubs models this callable as a nominal class, while grpcio
            # accepts ordinary callback functions at runtime.
            grpc_context.add_done_callback(cast("DoneCallback[Any, Any]", on_done))
            if grpc_context.cancelled():
                cancelled.set()
            return await self._handle_with_tracing(
                carrier, sid, req, sender, eof_after_first, request_iter,
            )
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            cancelled.set()
            if timer is not None:
                timer.cancel()
            request_cancelled.reset(cancelled_token)
            request_deadline.reset(deadline_token)

    async def _handle_with_tracing(
        self,
        carrier: dict[str, str],
        sid: str,
        req: ReqT,
        sender: "Sender[ResR]",
        eof_after_first: bool,
        request_iter: Optional[AsyncIterator[ReqT]] = None,
    ) -> Optional[Exception]:
        if self._tracing is None:
            return await self._handle_common_inner(
                sid, req, sender, eof_after_first, request_iter
            )
        trace_requested = bool(carrier.get('x-trace')) or (
            data_source_endpoint_tracing_enabled(
                self._endpoint.environment, self._endpoint.id,
            )
        )
        has_remote_parent = bool(carrier.get('traceparent'))
        if not trace_requested and not has_remote_parent:
            return await self._handle_common_inner(
                sid, req, sender, eof_after_first, request_iter
            )
        with self._tracing.extract(carrier) as remote_sampled:
            with sampling_scope(trace_requested or remote_sampled):
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
        """Keep reservation and callback ownership through terminal handling."""
        sid_token = request_stream_id.set(sid)
        span = NOOP_SPAN
        span_scope = None
        result: Optional[_GrpcResult[HandlerState, T, ResR, R, E]] = None
        reserved = False
        ep: InputEndpoint = self._endpoint
        try:
            if self._tracer is not None and sampling_enabled():
                _, span = start_endpoint_span(
                    self._tracer, "grpc.input", self._sc.stream.name, self._endpoint.name,
                    pipeline_name=self._pipeline_name, component_name=self._component_name,
                )
            if span is not NOOP_SPAN:
                span_scope = span.scoped()
                span_scope.__enter__()
            try:
                handler_state = await self._handler.begin_request(self._sc)
            except Exception as err:
                if span is not NOOP_SPAN:
                    span_error(span, err)
                    span.add_event("begin_request.error", string_attr("error", str(err)))
                return err
            if span is not NOOP_SPAN:
                span.add_event("begin_request")
            started = ep.on_request_start()
            if isinstance(sender, (_UnarySender, _StreamSender)):
                sender._span = span
            if span is not NOOP_SPAN:
                span_attrs(span, string_attr("stream_id", sid), bool_attr("has_result", self._has_result))

            result = _GrpcResult(handler_state, sender, span)
            if isinstance(sender, _ClientStreamingSender):
                sender._done = result.done
            try:
                if self._pending is None:
                    raise RuntimeError("gRPC endpoint consumer is not started")
                self._pending.set(sid, result)
                reserved = True
            except Exception as err:
                ep.on_begin_request_failed(err)
                if span is not NOOP_SPAN:
                    span_error(span, err)
                    span.add_event("request_rejected", string_attr("error", str(err)))
                try:
                    await self._handler.end_request(self._sc, err, handler_state)
                except Exception:
                    # A rejected opening cannot replace the admission error.
                    pass
                ep.on_request_end(started, err)
                return err

            result_ctx: ResultContext[HandlerState, T, ResR, R, E]
            if self._has_result:
                ep.on_pending_add(sid)
                result_ctx = result
            else:
                result_ctx = cast(ResultContext, _NOOP_RESULT_CONTEXT)

            error: Optional[Exception] = None
            try:
                if request_iter is not None:
                    async for message in request_iter:
                        await self._handler.consume_message(
                            self._sc, handler_state, message, result_ctx, sender,
                        )
                else:
                    await self._handler.consume_message(
                        self._sc, handler_state, req, result_ctx, sender,
                    )
                if span is not NOOP_SPAN:
                    span.add_event("consume_message")
                # Failed reads or ConsumeMessage calls do not signal EOF.
                self._handler.eof(self._sc, handler_state)
                if span is not NOOP_SPAN:
                    span.add_event("eof")
                if not self._has_result and isinstance(sender, _ClientStreamingSender):
                    # With no result path, Go sends the zero response before
                    # EndRequest. A previous Send already owns the response.
                    await sender.send(cast(ResR, None))
                if self._has_result:
                    completion: asyncio.Future[Any] = (
                        sender._future
                        if isinstance(sender, _UnarySender) and eof_after_first
                        else result._done
                    )
                    try:
                        await asyncio.shield(completion)
                    except asyncio.CancelledError:
                        if not completion.done() or completion.cancelled():
                            raise
                    if span is not NOOP_SPAN:
                        span.add_event(
                            "result_received"
                            if isinstance(sender, _UnarySender) and eof_after_first
                            else "done_received"
                        )
            except asyncio.CancelledError:
                cancelled = request_cancelled.get()
                if cancelled is not None:
                    cancelled.set()
                error = RuntimeError("gRPC request context cancelled")
                if span is not NOOP_SPAN:
                    span_error(span, error)
                    span.add_event("context_cancelled", string_attr("error", str(error)))
            except Exception as err:
                error = err
                if span is not NOOP_SPAN:
                    span_error(span, err)

            result.closed = True
            active_result = result

            async def finish_request() -> Optional[Exception]:
                async with active_result.lifetime.write_lock():
                    # EndRequest decides whether the operation error is
                    # handled. Cancellation must not interrupt this ownership
                    # boundary or release callbacks still using the state.
                    end_error: Optional[Exception] = None
                    try:
                        await self._handler.end_request(self._sc, error, handler_state)
                    except Exception as err:
                        end_error = err
                        if span is not NOOP_SPAN:
                            span_error(span, err)
                    ep.on_request_end(started, end_error)
                    return end_error

            finalization = asyncio.create_task(finish_request())
            while True:
                try:
                    return await asyncio.shield(finalization)
                except asyncio.CancelledError:
                    cancelled = request_cancelled.get()
                    if cancelled is not None:
                        cancelled.set()
                    if finalization.done():
                        return finalization.result()
        finally:
            if result is not None:
                result.closed = True
                result._callbacks.clear()
            if reserved and self._pending is not None:
                self._pending.pop(sid)
                if self._has_result:
                    ep.on_pending_remove(sid)
            try:
                if span_scope is not None:
                    span_scope.__exit__(*sys.exc_info())
                if span is not NOOP_SPAN:
                    span.end()
            finally:
                request_stream_id.reset(sid_token)


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
    tracing = stream.environment.tracing
    tracing_enabled = tracing is not None
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, tracing, tracer
    )

    async def _handle(request: ReqT, context: grpc.aio.ServicerContext[ReqT, ResR]) -> ResR:
        carrier = _grpc_metadata(context, tracing_enabled)
        sid = carrier.get('x-stream-id') or new_stream_id()
        sender = _UnarySender[ResR]()
        err = await ec._handle_common(context, carrier, sid, request, sender, eof_after_first=True)
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))
        return sender._future.result() if sender._future.done() else cast(ResR, None)

    return ec, _handle


def make_grpc_server_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], ServerStreamingHandler[ReqT, ResR]]":
    """Server-streaming gRPC source: one request → N responses."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    tracing = stream.environment.tracing
    tracing_enabled = tracing is not None
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, tracing, tracer
    )

    async def _handle(request: ReqT, context: grpc.aio.ServicerContext[ReqT, ResR]) -> None:
        carrier = _grpc_metadata(context, tracing_enabled)
        sid = carrier.get('x-stream-id') or new_stream_id()
        sender = _StreamSender[ResR](context.write)
        err = await ec._handle_common(context, carrier, sid, request, sender, eof_after_first=True)
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
    tracing = stream.environment.tracing
    tracing_enabled = tracing is not None
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, tracing, tracer
    )

    async def _handle(
        request_iterator: AsyncIterator[ReqT],
        context: grpc.aio.ServicerContext[ReqT, ResR],
    ) -> ResR:
        carrier = _grpc_metadata(context, tracing_enabled)
        sid = carrier.get('x-stream-id') or new_stream_id()
        sender = _ClientStreamingSender[ResR]()
        err = await ec._handle_common(
            context, carrier, sid, cast(ReqT, None), sender, eof_after_first=False,
            request_iter=request_iterator,
        )
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))
        return sender._future.result() if sender._future.done() else cast(ResR, None)

    return ec, _handle


def make_grpc_bidi_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
) -> "tuple[Consumer[T], BidiStreamingHandler[ReqT, ResR]]":
    """Bidi-streaming gRPC source: N requests → N responses."""
    ds = _get_or_create_datasource(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    tracer = _make_tracer(stream, stream.environment)
    tracing = stream.environment.tracing
    tracing_enabled = tracing is not None
    ec = _GrpcTypedEndpointConsumer[HandlerState, ReqT, ResR, T, R, E](
        ep, stream, handler, tracing, tracer
    )

    async def _handle(
        request_iterator: AsyncIterator[ReqT],
        context: grpc.aio.ServicerContext[ReqT, ResR],
    ) -> None:
        carrier = _grpc_metadata(context, tracing_enabled)
        sid = carrier.get('x-stream-id') or new_stream_id()
        sender = _StreamSender[ResR](context.write)
        err = await ec._handle_common(
            context, carrier, sid, cast(ReqT, None), sender, eof_after_first=False,
            request_iter=request_iterator,
        )
        sender.close()
        if err is not None:
            await context.abort(grpc.StatusCode.INTERNAL, str(err))

    return ec, _handle

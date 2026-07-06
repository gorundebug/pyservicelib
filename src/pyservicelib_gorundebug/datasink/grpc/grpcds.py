#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

"""
gRPC datasink endpoint consumers for all four streaming modes.

Equivalent to Go's datasink/grpc/ package (grpc.go + nostreaming.go +
serverstreaming.go + clientstreaming.go + bidistreaming.go).

Each make_* function takes a gRPC client function (matching the protoc-generated
stub signature) and returns a Consumer[T] that calls it per pipeline value.
"""

import asyncio
from typing import Optional, Protocol, Any, AsyncIterator, Callable, cast

import grpc.aio  # type: ignore[import-untyped]

from ...runtime.common import (
    Consumer, SinkStreamContext, CollectFunc, TypedSinkStreamWithResult,
    ServiceExecutionEnvironment, SinkEndpoint, OutputEndpointConsumer,
)
from ...runtime.context import Context
from ...runtime.context.request import stream_id_from_context, with_stream_id, new_stream_id
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint


# Client function type aliases — match protoc-generated stub signatures

# Unary: one request → one response
NoStreamingClientFn = Callable[[Any], Any]  # (request) -> Awaitable[response]
# Server-streaming: one request → async iterator of responses
ServerStreamingClientFn = Callable[[Any], Any]  # (request) -> async stream
# Client-streaming: async iterator of requests → one response
ClientStreamingClientFn = Callable[[], Any]  # () -> stream stub
# Bidi-streaming: async iterator of requests → async iterator of responses
BidiStreamingClientFn = Callable[[], Any]  # () -> stream stub


class Sender[ReqT](Protocol):
    """Sends a gRPC request. Equivalent to Go's datasink/grpc Sender."""
    async def send(self, req: ReqT) -> None: ...


class ResultContext(Protocol):
    """Signals end of request stream. Equivalent to Go's datasink/grpc ResultContext."""
    def done(self) -> None: ...


class _NopResultContext:
    def done(self) -> None:
        pass


class _DoneResultContext:
    _event: asyncio.Event

    def __init__(self):
        self._event = asyncio.Event()

    def done(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()


class EndpointHandler[HandlerState, ReqT, ResR, T, R, E](Protocol):
    """
    User-supplied handler for gRPC sink calls.

    Lifecycle (unary/server-streaming — one call per Consume):
        begin_request → consume_message → [gRPC call] → handle_response(N) → end_request

    Lifecycle (bidi/client-streaming — one stream per streamID):
        begin_request → consume_message (N) → handle_response(M) → end_request

    Equivalent to Go's datasink/grpc EndpointHandler.
    """

    async def begin_request(
        self,
        sc: SinkStreamContext[T, R, E],
    ) -> "tuple[HandlerState, Optional[Exception]]": ...

    async def consume_message(
        self,
        sc: SinkStreamContext[T, R, E],
        handler_state: HandlerState,
        value: T,
        sender: Sender[ReqT],
        result_ctx: ResultContext,
    ) -> Optional[Exception]: ...

    async def handle_response(
        self,
        sc: SinkStreamContext[T, R, E],
        handler_state: HandlerState,
        response: ResR,
    ) -> Optional[Exception]: ...

    async def end_request(
        self,
        sc: SinkStreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


class _RequestSender[ReqT](Sender[ReqT]):
    """Stores the request for later use (unary/server-streaming)."""
    req: Optional[ReqT]

    def __init__(self):
        self.req = None

    async def send(self, req: ReqT) -> None:
        self.req = req


class _GrpcStreamSender[ReqT](Sender[ReqT]):
    """Forwards Send to the live gRPC stream (bidi/client-streaming)."""
    _write_fn: Any
    _lock: asyncio.Lock
    _active: bool

    def __init__(self, write_fn: Any):
        self._write_fn = write_fn
        self._lock = asyncio.Lock()
        self._active = True

    async def send(self, req: ReqT) -> None:
        async with self._lock:
            if not self._active:
                raise RuntimeError("gRPC stream is closed")
            await self._write_fn(req)

    def close(self) -> None:
        self._active = False


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
    _client_fn: Any
    _sc: SinkStreamContext[T, R, E]

    def __init__(
        self,
        endpoint: _GrpcSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, ReqT, ResR, T, R, E],
        client_fn: Any,
    ):
        self._endpoint = endpoint
        self._stream = stream
        self._handler = handler
        self._client_fn = client_fn

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

    async def _begin(self) -> "tuple[HandlerState, Optional[Exception]]":
        return await self._handler.begin_request(self._sc)

    async def _end(self, err: Optional[Exception], handler_state: HandlerState) -> None:
        await self._handler.end_request(self._sc, err, handler_state)


class _NoStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):

    async def consume(self, value: T) -> None:
        handler_state, err = await self._begin()
        if err is not None:
            return

        sender = _RequestSender[ReqT]()
        err = await self._handler.consume_message(
            self._sc, handler_state, value, sender, _NopResultContext()
        )
        if err is not None:
            await self._end(err, handler_state)
            return

        if sender.req is None:
            await self._end(ValueError("no gRPC request set by handler"), handler_state)
            return

        # Propagate stream_id via metadata
        sid = stream_id_from_context()
        metadata = [('x-stream-id', sid)] if sid else []

        try:
            resp = await self._client_fn(sender.req, metadata=metadata)
        except Exception as e:
            await self._end(e, handler_state)
            return

        err = await self._handler.handle_response(self._sc, handler_state, resp)
        await self._end(err, handler_state)


class _ServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):

    async def consume(self, value: T) -> None:
        handler_state, err = await self._begin()
        if err is not None:
            return

        sender = _RequestSender[ReqT]()
        err = await self._handler.consume_message(
            self._sc, handler_state, value, sender, _NopResultContext()
        )
        if err is not None:
            await self._end(err, handler_state)
            return

        if sender.req is None:
            await self._end(ValueError("no gRPC request set by handler"), handler_state)
            return

        sid = stream_id_from_context()
        metadata = [('x-stream-id', sid)] if sid else []

        try:
            grpc_stream = self._client_fn(sender.req, metadata=metadata)
        except Exception as e:
            await self._end(e, handler_state)
            return

        async for resp in grpc_stream:
            err = await self._handler.handle_response(self._sc, handler_state, resp)
            if err is not None:
                await self._end(err, handler_state)
                return

        await self._end(None, handler_state)


class _ClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):

    async def consume(self, value: T) -> None:
        handler_state, err = await self._begin()
        if err is not None:
            return

        sid = stream_id_from_context()
        metadata = [('x-stream-id', sid)] if sid else []

        try:
            grpc_stream = await self._client_fn(metadata=metadata)
        except Exception as e:
            await self._end(e, handler_state)
            return

        done_ctx = _DoneResultContext()
        sender = _GrpcStreamSender[ReqT](grpc_stream.write)

        err = await self._handler.consume_message(
            self._sc, handler_state, value, sender, done_ctx
        )
        if err is not None:
            sender.close()
            await self._end(err, handler_state)
            return

        # Wait for done signal then close send side
        await done_ctx.wait()
        sender.close()
        await grpc_stream.done_writing()

        resp = await grpc_stream
        err = await self._handler.handle_response(self._sc, handler_state, resp)
        await self._end(err, handler_state)


class _BidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](
        _GrpcSinkEndpointConsumer[HandlerState, ReqT, ResR, T, R, E]):

    async def consume(self, value: T) -> None:
        handler_state, err = await self._begin()
        if err is not None:
            return

        sid = stream_id_from_context()
        metadata = [('x-stream-id', sid)] if sid else []

        try:
            grpc_stream = await self._client_fn(metadata=metadata)
        except Exception as e:
            await self._end(e, handler_state)
            return

        done_ctx = _DoneResultContext()
        sender = _GrpcStreamSender[ReqT](grpc_stream.write)

        # Start receiving responses concurrently
        recv_err: Optional[Exception] = None

        async def _recv_loop() -> None:
            nonlocal recv_err
            try:
                async for resp in grpc_stream:
                    e = await self._handler.handle_response(self._sc, handler_state, resp)
                    if e is not None:
                        recv_err = e
                        return
            except Exception as e:
                recv_err = e

        recv_task = asyncio.create_task(_recv_loop())

        err = await self._handler.consume_message(
            self._sc, handler_state, value, sender, done_ctx
        )
        if err is not None:
            sender.close()
            recv_task.cancel()
            try:
                await recv_task
            except asyncio.CancelledError:
                pass
            await self._end(err, handler_state)
            return

        await done_ctx.wait()
        sender.close()
        await grpc_stream.done_writing()
        await recv_task

        await self._end(recv_err, handler_state)


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


# ---------------------------------------------------------------------------
# Factory functions — one per gRPC streaming mode
# ---------------------------------------------------------------------------

def make_grpc_no_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: Any,
) -> Consumer[T]:
    """
    Unary gRPC sink: one request → one response per Consume.
    client_fn: async def method(request, metadata=()) -> response
    """
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _NoStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](ep, stream, handler, client_fn)


def make_grpc_server_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: Any,
) -> Consumer[T]:
    """
    Server-streaming gRPC sink: one request → N responses per Consume.
    client_fn: def method(request, metadata=()) -> async_stream
    """
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _ServerStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](ep, stream, handler, client_fn)


def make_grpc_client_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: Any,
) -> Consumer[T]:
    """
    Client-streaming gRPC sink: N requests → one response per Consume.
    client_fn: async def method(metadata=()) -> stream_stub
    """
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _ClientStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](ep, stream, handler, client_fn)


def make_grpc_bidi_streaming_endpoint_consumer[HandlerState, ReqT, ResR, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, ReqT, ResR, T, R, E]",
    client_fn: Any,
) -> Consumer[T]:
    """
    Bidi-streaming gRPC sink: N requests → M responses per Consume.
    client_fn: async def method(metadata=()) -> stream_stub
    """
    ds = _get_or_create_datasink(stream.endpoint_id, stream.environment)
    ep = _get_or_create_endpoint(stream, ds)
    return _BidiStreamingSinkConsumer[HandlerState, ReqT, ResR, T, R, E](ep, stream, handler, client_fn)

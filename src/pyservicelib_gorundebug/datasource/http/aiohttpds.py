#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import logging
from abc import abstractmethod, ABC
from typing import cast, Optional, Any, Protocol

import aiohttp.log
from aiohttp import web

from ...runtime.common import (
    TypedInputStream, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.environment.tracing import (
    Tracer, Tracing, Span, start_span, span_event, span_error, string_attr,
    sampling_enabled, sampling_scope,
)

_PENDING_ROTATION_INTERVAL = 30.0


class AIOHttpDataSource(InputDataSource):

    _app: Optional[web.Application]
    _runner: Optional[web.AppRunner]
    _site: Optional[web.TCPSite]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)
        self._app = None
        self._runner = None
        self._site = None
        if getattr(self.data_connector, 'use_dedicated_listener', False):
            native = getattr(env.log, 'native_logger', None)
            self._app = web.Application(
                logger=cast(logging.Logger, native) if isinstance(native, logging.Logger)
                else aiohttp.log.web_logger)

    async def start(self, ctx: Context) -> None:
        if self._app is not None:
            dc = self.data_connector
            if getattr(dc, 'host', None) is None:
                raise ValueError(f"Host required for http data source '{dc.name}'")
            if getattr(dc, 'port', None) is None:
                raise ValueError(f"Port required for http data source '{dc.name}'")
            runner = web.AppRunner(self._app)
            await runner.setup()
            self._runner = runner
            site = web.TCPSite(runner=runner, host=dc.host, port=dc.port)
            await site.start()
            self._site = site

        for ep in self.endpoints:
            await ep.start(ctx)  # type: ignore[attr-defined]

    async def stop(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await ep.stop(ctx)  # type: ignore[attr-defined]

        if self._runner is not None:
            await self._runner.cleanup()

    def add_handler(self, method: str, path: str, handler: Any) -> None:
        if self._app is not None:
            self._app.router.add_route(method=method, path=path, handler=handler)
        else:
            self.environment.register_http_handler(path, handler, method)


class HandlerData:
    """Carries the aiohttp request and a settable response slot. Equivalent to Go's HandlerData."""

    request: web.Request
    _response: "asyncio.Future[web.Response]"

    def __init__(self, request: web.Request):
        self.request = request
        self._response = asyncio.get_event_loop().create_future()

    def set_response(self, response: web.Response) -> None:
        if not self._response.done():
            self._response.set_result(response)

    async def get_response(self) -> web.Response:
        return await self._response


class ResultCallback[HandlerState, T, R, E](Protocol):
    """Return True to deregister after this call; False to keep active."""
    def __call__(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
        data: HandlerData,
    ) -> bool: ...


class ResultContext[HandlerState, T, R, E](ABC):
    @abstractmethod
    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, R, E]",
    ) -> None: ...

    @abstractmethod
    def done(self) -> None: ...


class EndpointHandler[HandlerState, T, R, E](Protocol):
    """
    Equivalent to Go's datasource/http EndpointHandler.

    Lifecycle with result stream:
        begin_request → consume_message → [await done] → end_request

    Lifecycle without result stream:
        begin_request → consume_message → end_request
    """

    async def begin_request(
        self,
        sc: StreamContext[T, R, E],
        data: HandlerData,
    ) -> "tuple[HandlerData, HandlerState]": ...

    async def consume_message(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        data: HandlerData,
        result_ctx: "ResultContext[HandlerState, T, R, E]",
    ) -> None: ...

    def get_message_id(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        value: R,
    ) -> str: ...

    async def end_request(
        self,
        sc: StreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
        data: HandlerData,
    ) -> None: ...


class _HttpResult[HandlerState, T, R, E](ResultContext[HandlerState, T, R, E]):
    handler_state: HandlerState
    data: HandlerData
    _done: asyncio.Event
    _callbacks: dict[str, Any]
    _cb_lock: asyncio.Lock
    _span: Optional[Span]
    _once: bool

    def __init__(self, handler_state: HandlerState, data: HandlerData):
        self.handler_state = handler_state
        self.data = data
        self._done = asyncio.Event()
        self._callbacks = {}
        self._cb_lock = asyncio.Lock()
        self._span = None
        self._once = False

    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, R, E]",
    ) -> None:
        self._callbacks[message_id] = cb

    def done(self) -> None:
        if not self._once:
            self._once = True
            span_event(self._span, "done_called")
        self._done.set()


class _NetHTTPEndpoint(DataSourceEndpoint):
    _consumer: Optional["_NetHTTPTypedEndpointConsumer"]

    def __init__(self, datasource: AIOHttpDataSource, id_endpoint: int):
        cfg = datasource.environment.config.get_endpoint_config_by_id(id_endpoint)
        if not cfg.method:
            raise ValueError(f"Method required for endpoint '{cfg.name}'")
        if not cfg.path:
            raise ValueError(f"Path required for endpoint '{cfg.name}'")
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        self._consumer = None
        datasource.add_handler(cfg.method, cfg.path, self._handle)

    async def _handle(self, request: web.Request) -> web.Response:
        if self._consumer is None:
            return web.Response(status=503, text="no consumer registered")
        return await self._consumer.serve_http(request)

    async def start(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.stop(ctx)


class _ResultConsumerProxy[R](Consumer[R]):
    def __init__(self, consumer: "_NetHTTPTypedEndpointConsumer") -> None:  # type: ignore[type-arg]
        self._consumer = consumer

    async def consume(self, value: R) -> None:
        await self._consumer._consume_result(value)  # type: ignore[arg-type]


class _NetHTTPTypedEndpointConsumer[HandlerState, T, R, E](DataSourceEndpointConsumer[T, R, E]):
    _handler: EndpointHandler[HandlerState, T, R, E]
    _sc: StreamContext[T, R, E]
    _has_result: bool
    _pending: Optional[RotatingMap[str, _HttpResult[HandlerState, T, R, E]]]
    _tracer: Optional[Tracer]
    _tracing: Optional[Tracing]

    def __init__(
        self,
        endpoint: _NetHTTPEndpoint,
        stream: TypedInputStream[T, R, E],
        handler: EndpointHandler[HandlerState, T, R, E],
        tracing: Optional[Tracing],
    ):
        super().__init__(endpoint=endpoint, input_stream=stream)
        self._handler = handler
        self._has_result = stream.get_result_stream() is not None
        self._pending = None
        self._tracing = tracing
        self._tracer = (
            tracing.tracer(stream.environment.service_config.name)
            if tracing is not None
            else None
        )

        self._sc = StreamContext[T, R, E](
            stream=stream,
            result_stream=stream.get_result_stream(),
            collect=CollectFunc[T](stream.consume),
            error_collect=CollectFunc[E](stream.error_stream.consume),
        )

        if self._has_result:
            stream.set_result_consumer(_ResultConsumerProxy[R](self))  # type: ignore[arg-type]

        endpoint._consumer = self
        endpoint.add_endpoint_consumer(self)

    async def start(self, ctx: Context) -> None:
        if self._has_result:
            self._pending = RotatingMap[str, Any](_PENDING_ROTATION_INTERVAL)
            await self._pending.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._pending is not None:
            await self._pending.stop(ctx)

    async def serve_http(self, request: web.Request) -> web.Response:
        if self._tracing is None:
            return await self._serve_http(request)
        carrier = {key.lower(): value for key, value in request.headers.items()}
        with self._tracing.extract(carrier) as remote_sampled:
            with sampling_scope(
                bool(request.headers.get('x-trace')) or remote_sampled
            ):
                return await self._serve_http(request)

    async def _serve_http(self, request: web.Request) -> web.Response:
        data = HandlerData(request)
        sid = request.headers.get('x-stream-id') or new_stream_id()
        with_stream_id(sid)

        ep = cast(DataSourceEndpoint, self._endpoint)
        cfg = ep.config
        method = getattr(cfg, 'method', '') or ''
        path = getattr(cfg, 'path', '') or ''

        _, span = start_span(
            self._tracer if sampling_enabled() else None,
            "http.input",
            string_attr("stream", self._input_stream.name),
            string_attr("endpoint", ep.name),
            string_attr("method", method),
            string_attr("path", path),
        )
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                try:
                    handler_data, handler_state = await self._handler.begin_request(self._sc, data)
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    if not data._response.done():
                        data.set_response(web.Response(status=500, text=str(err)))
                    return await data.get_response()
                span_event(span, "begin_request")

                result: _HttpResult[HandlerState, T, R, E] = _HttpResult(handler_state, handler_data)
                result._span = span
                if self._has_result and self._pending is not None:
                    self._pending.set(sid, result)
                    ep.on_pending_add(sid)

                try:
                    await self._handler.consume_message(self._sc, handler_state, handler_data, result)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    if self._has_result and self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                    await self._handler.end_request(self._sc, err, handler_state, handler_data)
                    if not data._response.done():
                        data.set_response(web.Response(status=500, text=str(err)))
                    return await data.get_response()
                span_event(span, "consume_message")

                if not self._has_result:
                    await self._handler.end_request(self._sc, None, handler_state, handler_data)
                    if not data._response.done():
                        data.set_response(web.Response())
                    return await data.get_response()

                try:
                    await result._done.wait()
                    span_event(span, "done_received")
                except asyncio.CancelledError:
                    span_event(span, "context_cancelled")
                finally:
                    if self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)

                await self._handler.end_request(self._sc, None, handler_state, handler_data)
                if not data._response.done():
                    data.set_response(web.Response())
                return await data.get_response()
        finally:
            response = (
                data._response.result()
                if data._response.done()
                and not data._response.cancelled()
                and data._response.exception() is None
                else None
            )
            response_status = (
                str(response.status)
                if response is not None
                else None
            )
            response_body_size = (
                len(response.body)
                if response is not None
                and isinstance(response.body, (bytes, bytearray))
                else response.content_length
                if response is not None
                else None
            )
            ep.on_request_end(
                start_time,
                end_err,
                response_status,
                request.content_length,
                response_body_size,
            )
            span.end()

    async def _consume_result(self, value: R) -> None:
        if not self._has_result or self._pending is None:
            return
        ep = cast(DataSourceEndpoint, self._endpoint)
        sid = stream_id_from_context()
        if sid is None:
            ep.on_missing_stream_id()
            return
        result, found = self._pending.get(sid)
        if not found or result is None:
            ep.on_late_result(sid)
            return

        message_id = self._handler.get_message_id(self._sc, result.handler_state, value)

        async with result._cb_lock:
            cb = result._callbacks.pop(message_id, None)
            if cb is None:
                ep.on_duplicate_message_id(sid, message_id)
                span_event(result._span, "duplicate_message_id",
                           string_attr("message_id", message_id))
                return
            remove = cb(self._sc, result.handler_state, value, result.data)
            if not remove:
                result._callbacks[message_id] = cb


def make_net_http_endpoint_consumer[HandlerState, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, T, R, E]",
) -> Consumer[T]:
    """
    Equivalent to Go's MakeNetHTTPEndpointConsumer (datasource/http).
    """
    env = stream.environment
    cfg_ep = env.config.get_endpoint_config_by_id(stream.endpoint_id)
    datasource = env.get_datasource(cfg_ep.id_data_connector)
    if datasource is None:
        cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
        datasource = AIOHttpDataSource(cfg_ds.id, env)
        env.add_datasource(datasource)
    ds = cast(AIOHttpDataSource, datasource)

    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is None:
        endpoint = _NetHTTPEndpoint(datasource=ds, id_endpoint=stream.endpoint_id)
        ds.add_endpoint(endpoint)

    return _NetHTTPTypedEndpointConsumer[HandlerState, T, R, E](
        endpoint=cast(_NetHTTPEndpoint, endpoint),
        stream=stream,
        handler=handler,
        tracing=env.tracing,
    )

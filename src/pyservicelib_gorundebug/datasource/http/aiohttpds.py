#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import logging
from abc import abstractmethod, ABC
from typing import cast, Optional, Callable, Awaitable, Union, Any, get_origin, Protocol

import aiohttp.log
from aiohttp import web
from multidict import MultiMapping
from pydantic import BaseModel

from ...runtime.common import (
    TypedInputStream, InputEndpoint, ServiceExecutionEnvironment,
    Consumer, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint
from ...runtime.store.rotatingmap import RotatingMap

_PENDING_ROTATION_INTERVAL = 30.0  # seconds


def _flatten_params(*param_dicts: MultiMapping) -> dict[str, Union[Any, list[Any]]]:
    result: dict[str, Union[Any, list[Any]]] = {}
    for params in param_dicts:
        for key in params:
            values = params.getall(key)
            if key in result:
                if isinstance(result[key], list):
                    result[key].extend(values)
                else:
                    result[key] = [result[key]] + values
            else:
                result[key] = values[0] if len(values) == 1 else values
    return result


class HTTPEndpointRequestData:
    _request: web.Request
    _body: Optional[bytes]
    _json: Optional[bytes]
    _form: Optional[dict[str, Union[Any, list[Any]]]]

    def __init__(self, request: web.Request):
        self._request = request
        self._body = None
        self._form = None
        self._json = None

    @property
    async def body(self) -> bytes:
        if self._body is None:
            self._body = await self._request.read()
        return self._body  # type: ignore[return-value]

    @property
    async def json(self) -> Any:
        if self._json is None:
            self._json = await self._request.json()
        return self._json

    @property
    async def form(self):
        if self._form is None:
            self._form = _flatten_params(self._request.query, await self._request.post())
        return self._form


class AIOHttpDataSource(InputDataSource):

    _app: web.Application
    _runner: Optional[web.AppRunner]
    _site: Optional[web.TCPSite]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)

        native = getattr(env.log, 'native_logger', None)
        self._app = web.Application(
            logger=cast(logging.Logger, native) if isinstance(native, logging.Logger)
            else aiohttp.log.web_logger)
        self._runner = None
        self._site = None

    async def start(self, ctx: Context) -> None:
        if self.data_connector.host is None:
            raise ValueError(f"Host property can not be None for http data source '{self.data_connector.name}'")
        if self.data_connector.port is None:
            raise ValueError(f"Port property can not be None for http data source '{self.data_connector.name}'")
        runner = web.AppRunner(self._app)
        await runner.setup()
        self._runner = runner
        site = web.TCPSite(runner=runner, host=self.data_connector.host, port=self.data_connector.port)
        await site.start()
        self._site = site

        for ep in self.endpoints:
            await ep.start(ctx)  # type: ignore[union-attr]

    async def stop(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await ep.stop(ctx)  # type: ignore[union-attr]

        if self._runner is not None:
            await self._runner.cleanup()

    def add_handler(self, method: str, path: str, handler: Callable[[web.Request], Awaitable[web.Response]]) -> None:
        self._app.router.add_route(method=method, path=path, handler=handler)


# ---------------------------------------------------------------------------
# Simple API — original AIOHttpEndpointConsumer
# ---------------------------------------------------------------------------

class AIOHttpEndpoint(DataSourceEndpoint):
    def __init__(self, datasource: AIOHttpDataSource, id_endpoint: int):
        cfg = datasource.environment.config.get_endpoint_config_by_id(id_endpoint)
        if cfg.method is None:
            raise ValueError(f"Method property can not be None for endpoint '{cfg.name}'")
        if cfg.path is None:
            raise ValueError(f"Path property can not be None for endpoint '{cfg.name}'")
        if cfg.format is None:
            raise ValueError(f"Format property can not be None for endpoint '{cfg.name}'")
        elif (cfg.method == "POST" and cfg.format not in ["json", "form"] or
              cfg.method == "GET" and cfg.format != "form"):
            raise ValueError(f"Format property has invalid value '{cfg.format}' for endpoint '{cfg.name}'")

        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_handler(cfg.method, cfg.path, self.handler)

    async def handler(self, request: web.Request) -> web.Response:
        request_data = HTTPEndpointRequestData(request)
        try:
            for ec in self.endpoint_consumers:
                await cast("AIOHttpEndpointConsumer", ec).endpoint_request(request_data)
        except web.HTTPException as e:
            return web.Response(text=f"HTTP error occurred: {e}", status=400)
        except ValueError as e:
            return web.Response(text=f"Value error: {e}", status=400)
        except Exception as e:
            return web.Response(text=f"An unexpected error occurred: {e}", status=500)

        return web.Response()

    async def start(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast("AIOHttpEndpointConsumer", ec).start(ctx)

    async def stop(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast("AIOHttpEndpointConsumer", ec).stop(ctx)


class EndpointRequestConsumer(ABC):

    @abstractmethod
    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
       pass


class JsonRequestEndpointConsumer[T](EndpointRequestConsumer):
    _endpoint_consumer: "AIOHttpEndpointConsumer[T]"

    def __init__(self, endpoint_consumer: "AIOHttpEndpointConsumer[T]"):
        self._endpoint_consumer = endpoint_consumer

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        json = await request_data.json
        if self._endpoint_consumer.reader is None:
            value = cast(T, cast(BaseModel, self._endpoint_consumer.value_type).model_validate_json(json))
        else:
            value = self._endpoint_consumer.reader.from_dict(json)
        await self._endpoint_consumer.consume(value)


class FormRequestEndpointConsumer[T](EndpointRequestConsumer):
    _endpoint_consumer: "AIOHttpEndpointConsumer[T]"

    def __init__(self, endpoint_consumer: "AIOHttpEndpointConsumer[T]"):
        self._endpoint_consumer = endpoint_consumer

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        form = await request_data.form
        if self._endpoint_consumer.reader is None:
            value = self._endpoint_consumer.value_type(**form)
        else:
            value = self._endpoint_consumer.reader.from_dict(form)
        await self._endpoint_consumer.consume(value)


class AIOHttpEndpointConsumer[T](DataSourceEndpointConsumer[T]):
    _orig_type: type
    _request_endpoint_consumer: EndpointRequestConsumer

    def __init__(self, input_stream: TypedInputStream[T]):
        endpoint = AIOHttpEndpointConsumer.get_aiohttp_datasource_endpoint(input_stream.endpoint_id,
                                                                            input_stream.environment)
        super().__init__(endpoint=endpoint, input_stream=input_stream)
        endpoint.add_endpoint_consumer(self)
        if self.endpoint.config.format == "json":
            self._request_endpoint_consumer = JsonRequestEndpointConsumer(self)
        else:
            self._request_endpoint_consumer = FormRequestEndpointConsumer(self)

    @property
    def value_type(self) -> type:
        return self._orig_type

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        await self._request_endpoint_consumer.endpoint_request(request_data)

    async def start(self, ctx: Context):
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        self._orig_type = get_origin(genetic_type)
        if self._orig_type is None:
            self._orig_type = genetic_type
        if not issubclass(self._orig_type, BaseModel):
            raise ValueError(f"Invalid type value for endpoint {self.endpoint.name}. Use pydantic define custom reader.")

    async def stop(self, ctx: Context):
        pass

    @classmethod
    def get_aiohttp_datasource(cls, id_connector: int, env: ServiceExecutionEnvironment) -> AIOHttpDataSource:
        datasource = env.get_datasource(id_connector)
        if datasource is not None:
            return cast(AIOHttpDataSource, datasource)
        cfg = env.config.get_data_connector_by_id(id_connector)
        aiohttp_datasource = AIOHttpDataSource(cfg.id, env)
        env.add_datasource(aiohttp_datasource)
        return aiohttp_datasource

    @classmethod
    def get_aiohttp_datasource_endpoint(cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> InputEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        datasource = cls.get_aiohttp_datasource(cfg.id_data_connector, env)
        endpoint = datasource.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        aiohttp_endpoint = AIOHttpEndpoint(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_endpoint(aiohttp_endpoint)
        return aiohttp_endpoint


# ---------------------------------------------------------------------------
# Rich EndpointHandler API — matches Go's datasource/http/nethttp.go
# ---------------------------------------------------------------------------

class HandlerData:
    """Carries the aiohttp request and a settable response slot for handlers."""

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
    """Return True to deregister; False to keep active."""
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
    User-supplied handler for HTTP source requests.

    Lifecycle with result stream:
        begin_request → consume_message → [await done] → end_request

    Lifecycle without result stream:
        begin_request → consume_message → end_request

    The handler writes responses via data.set_response().
    """

    async def begin_request(
        self,
        sc: StreamContext[T, R, E],
        data: HandlerData,
    ) -> "tuple[HandlerData, HandlerState, Optional[Exception]]": ...

    async def consume_message(
        self,
        sc: StreamContext[T, R, E],
        handler_state: HandlerState,
        data: HandlerData,
        result_ctx: "ResultContext[HandlerState, T, R, E]",
    ) -> Optional[Exception]: ...

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

    def __init__(self, handler_state: HandlerState, data: HandlerData):
        self.handler_state = handler_state
        self.data = data
        self._done = asyncio.Event()
        self._callbacks = {}
        self._cb_lock = asyncio.Lock()

    def set_result_callback(
        self,
        message_id: str,
        cb: "ResultCallback[HandlerState, T, R, E]",
    ) -> None:
        self._callbacks[message_id] = cb

    def done(self) -> None:
        self._done.set()


class _NetHTTPEndpoint(DataSourceEndpoint):
    """Endpoint for the typed EndpointHandler API — one consumer per endpoint."""

    _consumer: Optional["_NetHTTPTypedEndpointConsumer"]
    _method: str

    def __init__(self, datasource: AIOHttpDataSource, id_endpoint: int):
        cfg = datasource.environment.config.get_endpoint_config_by_id(id_endpoint)
        if not cfg.method:
            raise ValueError(f"Method required for endpoint '{cfg.name}'")
        if not cfg.path:
            raise ValueError(f"Path required for endpoint '{cfg.name}'")
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        self._consumer = None
        self._method = cfg.method
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

    def __init__(
        self,
        endpoint: _NetHTTPEndpoint,
        stream: TypedInputStream[T, R, E],
        handler: EndpointHandler[HandlerState, T, R, E],
    ):
        super().__init__(endpoint=endpoint, input_stream=stream)
        self._handler = handler
        self._has_result = stream.get_result_stream() is not None
        self._pending = None

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
        data = HandlerData(request)
        sid = request.headers.get('x-stream-id') or new_stream_id()
        with_stream_id(sid)

        handler_data, handler_state, err = await self._handler.begin_request(self._sc, data)
        if err is not None:
            await self._handler.end_request(self._sc, err, handler_state, handler_data)
            if not data._response.done():
                data.set_response(web.Response(status=500, text=str(err)))
            return await data.get_response()

        result: _HttpResult[HandlerState, T, R, E] = _HttpResult(handler_state, handler_data)
        if self._has_result and self._pending is not None:
            self._pending.set(sid, result)

        err = await self._handler.consume_message(self._sc, handler_state, handler_data, result)
        if err is not None:
            if self._has_result and self._pending is not None:
                self._pending.pop(sid)
            await self._handler.end_request(self._sc, err, handler_state, handler_data)
            if not data._response.done():
                data.set_response(web.Response(status=500, text=str(err)))
            return await data.get_response()

        if not self._has_result:
            await self._handler.end_request(self._sc, None, handler_state, handler_data)
            if not data._response.done():
                data.set_response(web.Response())
            return await data.get_response()

        # Wait for done signal
        try:
            await result._done.wait()
        except asyncio.CancelledError:
            pass
        finally:
            if self._pending is not None:
                self._pending.pop(sid)

        await self._handler.end_request(self._sc, None, handler_state, handler_data)
        if not data._response.done():
            data.set_response(web.Response())
        return await data.get_response()

    async def _consume_result(self, value: R) -> None:
        if not self._has_result or self._pending is None:
            return
        sid = stream_id_from_context()
        if sid is None:
            return
        result, found = self._pending.get(sid)
        if not found or result is None:
            return

        message_id = self._handler.get_message_id(self._sc, result.handler_state, value)

        async with result._cb_lock:
            cb = result._callbacks.get(message_id)
            if cb is None:
                return
            remove = cb(self._sc, result.handler_state, value, result.data)
            if remove:
                result._callbacks.pop(message_id, None)


def make_net_http_endpoint_consumer[HandlerState, T, R, E](
    stream: TypedInputStream[T, R, E],
    handler: "EndpointHandler[HandlerState, T, R, E]",
) -> Consumer[T]:
    """
    Creates an HTTP datasource endpoint consumer for the rich EndpointHandler API.

    Equivalent to Go's MakeNetHTTPEndpointConsumer. Returns a Consumer[T] that
    the stream uses to push decoded values. The aiohttp route is registered
    automatically on the shared AIOHttpDataSource.
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
    )

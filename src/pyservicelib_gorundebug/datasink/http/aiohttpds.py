#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from typing import Optional, Protocol, Any, cast

import aiohttp

from ...runtime.common import (
    Consumer, SinkStreamContext, CollectFunc, TypedSinkStreamWithResult,
    ServiceExecutionEnvironment, SinkEndpoint, OutputEndpointConsumer,
)
from ...runtime.context import Context
from ...runtime.context.request import stream_id_from_context
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint


class Requester:
    """
    Builds the outgoing HTTP request inside ConsumeMessage.
    Equivalent to Go's datasink/http Requester.
    """

    _session: aiohttp.ClientSession
    _request: Optional[aiohttp.ClientRequest]
    _method: Optional[str]
    _url: Optional[str]
    _kwargs: dict[str, Any]

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._method = None
        self._url = None
        self._kwargs = {}

    def new_request(self, method: str, url: str, **kwargs: Any) -> "Requester":
        """Configure the outgoing request. kwargs are passed to aiohttp.ClientSession.request."""
        self._method = method
        self._url = url
        self._kwargs = kwargs
        return self

    async def execute(self) -> aiohttp.ClientResponse:
        if self._method is None or self._url is None:
            raise ValueError("Requester.new_request() must be called before execute()")
        return await self._session.request(self._method, self._url, **self._kwargs)


class Response:
    """Wraps aiohttp.ClientResponse. Handler code doesn't need to import aiohttp."""

    def __init__(self, resp: aiohttp.ClientResponse):
        self._resp = resp

    @property
    def status(self) -> int:
        return self._resp.status

    @property
    def headers(self) -> Any:
        return self._resp.headers

    async def text(self, encoding: Optional[str] = None) -> str:
        return await self._resp.text(encoding=encoding)

    async def json(self, **kwargs: Any) -> Any:
        return await self._resp.json(**kwargs)

    async def read(self) -> bytes:
        return await self._resp.read()

    @property
    def raw(self) -> aiohttp.ClientResponse:
        return self._resp


class EndpointHandler[HandlerState, T, R, E](Protocol):
    """
    User-supplied handler for HTTP sink calls.

    Lifecycle (one HTTP call per Consume):
        begin_request → consume_message → [HTTP call] → handle_response → end_request

    Equivalent to Go's datasink/http EndpointHandler.
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
        req: Requester,
    ) -> Optional[Exception]: ...

    async def handle_response(
        self,
        sc: SinkStreamContext[T, R, E],
        handler_state: HandlerState,
        resp: Response,
    ) -> Optional[Exception]: ...

    async def end_request(
        self,
        sc: SinkStreamContext[T, R, E],
        err: Optional[Exception],
        handler_state: HandlerState,
    ) -> None: ...


class _AIOHttpSinkDataSink(OutputDataSink):

    _stop_tasks: list[asyncio.Task]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)
        self._stop_tasks = []

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast("_AIOHttpSinkEndpoint", ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_coros = [
            cast("_AIOHttpSinkEndpoint", ep).stop(ctx)
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
                    f"AIOHttp sink data sink '{self.name}' stopped by timeout."
                )


class _AIOHttpSinkEndpoint(DataSinkEndpoint):
    _consumer: Optional["_NetHTTPSinkEndpointConsumer"]

    def __init__(self, data_sink: _AIOHttpSinkDataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)
        self._consumer = None

    async def start(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.stop(ctx)


class _NetHTTPSinkEndpointConsumer[HandlerState, T, R, E](Consumer[T], OutputEndpointConsumer):
    _endpoint: _AIOHttpSinkEndpoint
    _stream: TypedSinkStreamWithResult[T, R, E]
    _handler: EndpointHandler[HandlerState, T, R, E]
    _session: Optional[aiohttp.ClientSession]
    _sc: SinkStreamContext[T, R, E]

    def __init__(
        self,
        endpoint: _AIOHttpSinkEndpoint,
        stream: TypedSinkStreamWithResult[T, R, E],
        handler: EndpointHandler[HandlerState, T, R, E],
    ):
        self._endpoint = endpoint
        self._stream = stream
        self._handler = handler
        self._session = None

        self._sc = SinkStreamContext[T, R, E](
            stream=stream,
            collect=CollectFunc[R](stream.consume_result),
            error_collect=CollectFunc[E](stream.error_stream.consume),
        )

        stream.set_sink_consumer(self)
        endpoint._consumer = self
        endpoint.add_endpoint_consumer(self)

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

    async def start(self, ctx: Context) -> None:
        self._session = aiohttp.ClientSession()

    async def stop(self, ctx: Context) -> None:
        if self._session is not None:
            await self._session.close()

    async def consume(self, value: T) -> None:
        if self._session is None:
            self._stream.environment.log.warn(
                f"HTTP sink consume called before start for stream '{self._stream.name}'"
            )
            return

        handler_state, err = await self._handler.begin_request(self._sc)
        if err is not None:
            return

        # Propagate stream_id as header
        sid = stream_id_from_context()
        req = Requester(self._session)
        if sid is not None:
            # Handler should set x-stream-id header; we expose sid via req metadata
            req._kwargs.setdefault('headers', {})
            if isinstance(req._kwargs['headers'], dict):
                req._kwargs['headers']['x-stream-id'] = sid

        err = await self._handler.consume_message(self._sc, handler_state, value, req)
        if err is not None:
            await self._handler.end_request(self._sc, err, handler_state)
            return

        try:
            resp_raw = await req.execute()
        except Exception as e:
            await self._handler.end_request(self._sc, e, handler_state)
            return

        async with resp_raw:
            err = await self._handler.handle_response(self._sc, handler_state, Response(resp_raw))

        if err is not None:
            await self._handler.end_request(self._sc, err, handler_state)
            return

        await self._handler.end_request(self._sc, None, handler_state)


def make_net_http_endpoint_consumer[HandlerState, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
    handler: "EndpointHandler[HandlerState, T, R, E]",
) -> Consumer[T]:
    """
    Creates an HTTP sink endpoint consumer that, for each pipeline value T,
    calls handler to build and send an outgoing HTTP request, then passes
    the response to handle_response.

    Equivalent to Go's MakeNetHTTPEndpointConsumer (datasink/http).
    """
    env = stream.environment
    cfg_ep = env.config.get_endpoint_config_by_id(stream.endpoint_id)

    datasink = env.get_datasink(cfg_ep.id_data_connector)
    if datasink is None:
        cfg_ds = env.config.get_data_connector_by_id(cfg_ep.id_data_connector)
        datasink = _AIOHttpSinkDataSink(cfg_ds.id, env)
        env.add_datasink(datasink)
    ds = cast(_AIOHttpSinkDataSink, datasink)

    endpoint = ds.get_endpoint(stream.endpoint_id)
    if endpoint is None:
        endpoint = _AIOHttpSinkEndpoint(data_sink=ds, id_endpoint=stream.endpoint_id)
        ds.add_endpoint(endpoint)

    return _NetHTTPSinkEndpointConsumer[HandlerState, T, R, E](
        endpoint=cast(_AIOHttpSinkEndpoint, endpoint),
        stream=stream,
        handler=handler,
    )

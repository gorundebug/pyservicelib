#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import asyncio
from typing import Protocol, Optional, Any, cast
from abc import ABC, abstractmethod

from ...runtime.common import (
    TypedSinkStream, ServiceExecutionEnvironment,
    DataSink, SinkEndpoint, Consumer, Collect, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.datasink import OutputDataSink, DataSinkEndpointConsumer, DataSinkEndpoint
from ...runtime.environment.tracing import (
    Tracer, start_span, span_event, span_error, string_attr, sampling_enabled,
)


class EndpointHandler[HandlerState, T, E](Protocol):
    """
    Handler for custom (local) sink messages.

    Pipeline lifecycle per value:
        GetStreamID → BeginRequest → ConsumeMessage → EndRequest

    GetStreamID returns a logical identifier for the incoming value.
    BeginRequest initialises per-message handler state.
    ConsumeMessage processes value T; push results back via result_stream.Out().
    EndRequest finalises the request.
    """

    def get_stream_id(self, ctx: Context, value: T) -> str:
        ...

    async def begin_request(
        self, ctx: Context, stream: TypedSinkStream[T, E],
    ) -> tuple[Context, HandlerState]:
        ...

    async def consume_message(
        self, ctx: Context, stream: TypedSinkStream[T, E],
        handler_state: HandlerState, value: T,
        result_stream: Collect[E],
    ) -> None:
        ...

    async def end_request(
        self, ctx: Context, stream: TypedSinkStream[T, E],
        err: Optional[Exception], handler_state: HandlerState,
    ) -> None:
        ...


class CustomEndpointConsumer(ABC):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass


class CustomSinkEndpoint(DataSinkEndpoint):
    _consumer: Optional[CustomEndpointConsumer]

    def __init__(self, data_sink: DataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)
        self._consumer = None

    async def start(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.stop(ctx)


class CustomDataSink(OutputDataSink):

    def __init__(self, id_connector: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=id_connector, env=env)

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast(CustomSinkEndpoint, ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_tasks: list[asyncio.Task[Any]] = []
        for ep in self.endpoints:
            stop_tasks.append(asyncio.create_task(cast(CustomSinkEndpoint, ep).stop(ctx)))
        try:
            await asyncio.wait_for(asyncio.gather(*stop_tasks), timeout=ctx.time_left)
        except (asyncio.TimeoutError, TypeError):
            self.environment.log.warn(f"Custom data sink '{self.name}' stopped by timeout.")


class _TypedCustomEndpointConsumer[HandlerState, T, E](
        DataSinkEndpointConsumer[T, E], Consumer[T], CustomEndpointConsumer):
    _handler: EndpointHandler[HandlerState, T, E]
    _ctx: Optional[Context]
    _tracer: Optional[Tracer]

    def __init__(self, endpoint: SinkEndpoint, sink_stream: TypedSinkStream[T, E],
                 handler: EndpointHandler[HandlerState, T, E]):
        DataSinkEndpointConsumer.__init__(self, endpoint=endpoint, sink_stream=sink_stream)
        self._handler = handler
        self._ctx = None
        env = sink_stream.environment
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None

    async def consume(self, value: T) -> None:
        ctx = self._ctx or Context()
        stream = self.stream
        ep = cast(DataSinkEndpoint, self._endpoint)

        _, span = start_span(
            self._tracer if sampling_enabled() else None,
            "local.output",
            string_attr("stream", stream.name),
            string_attr("endpoint", ep.name),
        )
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                handler_ctx, handler_state = await self._handler.begin_request(ctx, stream)
                span_event(span, "begin_request")

                rs: Collect[E] = CollectFunc[E](stream.error_stream.consume)
                try:
                    await self._handler.consume_message(
                        handler_ctx, stream, handler_state, value, rs)
                    span_event(span, "consume_message")
                    await self._handler.end_request(handler_ctx, stream, None, handler_state)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    await self._handler.end_request(handler_ctx, stream, err, handler_state)
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()

    async def start(self, ctx: Context) -> None:
        self._ctx = ctx

    async def stop(self, ctx: Context) -> None:
        pass


def make_custom_endpoint_consumer[HandlerState, T, E](
        sink_stream: TypedSinkStream[T, E],
        handler: EndpointHandler[HandlerState, T, E],
) -> Consumer[T]:
    env = sink_stream.environment
    endpoint = _get_or_create_sink_endpoint(sink_stream.endpoint_id, env)
    consumer: _TypedCustomEndpointConsumer[HandlerState, T, E] = (
        _TypedCustomEndpointConsumer(endpoint=endpoint, sink_stream=sink_stream, handler=handler)
    )
    sink_stream.set_sink_consumer(consumer)
    cast(CustomSinkEndpoint, endpoint)._consumer = consumer
    endpoint.add_endpoint_consumer(consumer)
    return consumer


def _get_or_create_datasink(id_connector: int, env: ServiceExecutionEnvironment) -> DataSink:
    datasink = env.get_datasink(id_connector)
    if datasink is not None:
        return datasink
    cfg = env.config.get_data_connector_by_id(id_connector)
    custom_datasink = CustomDataSink(cfg.id, env)
    env.add_datasink(custom_datasink)
    return custom_datasink


def _get_or_create_sink_endpoint(id_endpoint: int, env: ServiceExecutionEnvironment) -> SinkEndpoint:
    cfg = env.config.get_endpoint_config_by_id(id_endpoint)
    datasink = _get_or_create_datasink(cfg.id_data_connector, env)
    endpoint = datasink.get_endpoint(id_endpoint)
    if endpoint is not None:
        return endpoint
    custom_endpoint = CustomSinkEndpoint(data_sink=datasink, id_endpoint=id_endpoint)
    datasink.add_endpoint(custom_endpoint)
    return custom_endpoint

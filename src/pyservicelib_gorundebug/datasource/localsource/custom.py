#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import asyncio
from abc import ABC, abstractmethod
from typing import Protocol, Optional, Any, cast

from ...runtime.common import (
    Consume, Consumer, ServiceExecutionEnvironment, InputEndpoint, TypedInputStream,
    DataSource, StreamContext, CollectFunc,
)
from ...runtime.context import Context
from ...runtime.context.request import (
    new_stream_id, stream_id_from_context, with_stream_id,
)
from ...runtime.datasource import InputDataSource, DataSourceEndpoint, DataSourceEndpointConsumer
from ...runtime.store.rotatingmap import RotatingMap
from ...runtime.environment.tracing import (
    Tracer, Span, start_endpoint_span, span_event, span_error, string_attr,
)


class DataProducer[T](Protocol):

    async def start(self, ctx: Context, consumer: Consume[T]) -> None:
        ...

    async def stop(self, ctx: Context) -> None:
        ...


class ResultCallback[HandlerState, T, R, E](Protocol):

    def __call__(self, ctx: Context, sc: StreamContext[T, R, E],
                 handler_state: HandlerState, value: R) -> bool:
        ...


class ResultContext[HandlerState, T, R, E](ABC):

    __slots__ = ()

    @abstractmethod
    def set_result_callback(self, message_id: str,
                            cb: ResultCallback[HandlerState, T, R, E]) -> None:
        pass

    @abstractmethod
    def done(self) -> None:
        pass


class _NoopResultContext(ResultContext):
    __slots__ = ()

    def set_result_callback(self, message_id: str, cb: Any) -> None:
        del message_id, cb

    def done(self) -> None:
        pass


_NOOP_RESULT_CONTEXT = _NoopResultContext()


class EndpointHandler[HandlerState, T, R, E](Protocol):
    """
    Handler for custom (local) source messages.

    Lifecycle per value:
        BeginRequest → ConsumeMessage → EndRequest

    If a result stream exists (has_result=True):
        BeginRequest → ConsumeMessage → [await done] → EndRequest
    """

    def concurrency(self, sc: StreamContext[T, R, E]) -> int:
        ...

    async def begin_request(
        self, ctx: Context, sc: StreamContext[T, R, E],
    ) -> tuple[Context, HandlerState]:
        ...

    async def consume_message(
        self, ctx: Context, sc: StreamContext[T, R, E],
        handler_state: HandlerState, value: T,
        result_ctx: ResultContext[HandlerState, T, R, E],
    ) -> None:
        ...

    def get_message_id(
        self, ctx: Context, sc: StreamContext[T, R, E],
        handler_state: HandlerState, value: R,
    ) -> str:
        ...

    async def end_request(
        self, ctx: Context, sc: StreamContext[T, R, E],
        err: Optional[Exception], handler_state: HandlerState,
    ) -> None:
        ...


class _CustomResult[HandlerState, T, R, E](ResultContext[HandlerState, T, R, E]):
    __slots__ = (
        "_handler_ctx", "_handler_state", "_done", "_message_callbacks",
        "_span", "_once",
    )

    _handler_ctx: Context
    _handler_state: HandlerState
    _done: asyncio.Future[None]
    _message_callbacks: dict[str, Any]
    _span: Optional[Span]
    _once: bool

    def __init__(self, handler_ctx: Context, handler_state: HandlerState):
        self._handler_ctx = handler_ctx
        self._handler_state = handler_state
        self._done = asyncio.get_running_loop().create_future()
        self._message_callbacks = {}
        self._span = None
        self._once = False

    def set_result_callback(self, message_id: str,
                            cb: ResultCallback[HandlerState, T, R, E]) -> None:
        self._message_callbacks[message_id] = cb

    def done(self) -> None:
        if not self._once:
            self._once = True
            span_event(self._span, "done_called")
        if not self._done.done():
            self._done.set_result(None)


class CustomEndpointConsumer(ABC):

    @abstractmethod
    async def start(self, ctx: Context) -> None:
        pass

    @abstractmethod
    async def stop(self, ctx: Context) -> None:
        pass


class CustomInputEndpoint(DataSourceEndpoint):
    _consumer: Optional[CustomEndpointConsumer]

    def __init__(self, datasource: DataSource, id_endpoint: int):
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        self._consumer = None

    async def start(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.start(ctx)

    async def stop(self, ctx: Context) -> None:
        if self._consumer is not None:
            await self._consumer.stop(ctx)


class CustomDataSource(InputDataSource):

    def __init__(self, id_connector: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=id_connector, env=env)

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast(CustomInputEndpoint, ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_tasks: list[asyncio.Task[Any]] = []
        for ep in self.endpoints:
            stop_tasks.append(asyncio.create_task(cast(CustomInputEndpoint, ep).stop(ctx)))
        try:
            await asyncio.wait_for(asyncio.gather(*stop_tasks), timeout=ctx.time_left)
        except (asyncio.TimeoutError, TypeError):
            self.environment.log.warn(f"Custom data source '{self.name}' stopped by timeout.")


class _ResultConsumerProxy[R](Consumer[R]):
    def __init__(self, consumer: "TypedCustomEndpointConsumer") -> None:  # type: ignore[type-arg]
        self._consumer = consumer

    async def consume(self, value: R) -> None:
        await self._consumer._consume_result(value)  # type: ignore[arg-type]


class TypedCustomEndpointConsumer[HandlerState, T, R, E](
        DataSourceEndpointConsumer[T, R, E], CustomEndpointConsumer):
    _handler: EndpointHandler[HandlerState, T, R, E]
    _data_producer: DataProducer[T]
    _sc: StreamContext[T, R, E]
    _has_result: bool
    _concurrency_limit: int
    _semaphore: Optional[asyncio.Semaphore]
    _runner_task: Optional[asyncio.Task[Any]]
    _tasks: set[asyncio.Task[Any]]
    _stopped: bool
    _ctx: Optional[Context]
    _tracer: Optional[Tracer]
    _pending: Optional[RotatingMap[str, _CustomResult[HandlerState, T, R, E]]]

    def __init__(self, input_stream: TypedInputStream[T, R, E],
                 data_producer: DataProducer[T],
                 handler: EndpointHandler[HandlerState, T, R, E]):
        env = input_stream.environment
        endpoint = self._get_or_create_endpoint(
            id_endpoint=input_stream.endpoint_id, env=env)
        super().__init__(endpoint=endpoint, input_stream=input_stream)
        self._data_producer = data_producer
        self._handler = handler
        self._has_result = input_stream.get_result_stream() is not None
        self._concurrency_limit = 0
        self._semaphore = None
        self._runner_task = None
        self._tasks = set()
        self._stopped = False
        self._ctx = None
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        self._pending = None

        if self._has_result:
            input_stream.set_result_consumer(_ResultConsumerProxy[R](self))

        error_collect = CollectFunc[E](input_stream.error_stream.consume)
        collect = CollectFunc[T](self.out)
        self._sc = StreamContext[T, R, E](
            stream=input_stream,
            result_stream=input_stream.get_result_stream(),
            collect=collect,
            error_collect=error_collect,
        )

        cast(CustomInputEndpoint, endpoint)._consumer = self
        endpoint.add_endpoint_consumer(self)

    async def out(self, value: T) -> None:
        """Push a value into the pipeline (used by StreamContext.collect)."""
        await self._input_stream.consume(value)

    async def _endpoint_request(self, value: T) -> None:
        ctx = self._ctx or Context()
        ep = cast(DataSourceEndpoint, self._endpoint)

        _, span = start_endpoint_span(
            self._tracer,
            "local.input",
            self._input_stream.name,
            ep.name,
        )
        start_time = ep.on_request_start()
        end_err: Optional[Exception] = None
        try:
            with span.scoped():
                try:
                    handler_ctx, handler_state = await self._handler.begin_request(ctx, self._sc)
                except Exception as err:
                    ep.on_begin_request_failed(err)
                    span_error(span, err)
                    span_event(span, "begin_request.error", string_attr("error", str(err)))
                    end_err = err
                    return
                span_event(span, "begin_request")

                result: Optional[_CustomResult[HandlerState, T, R, E]] = None
                result_ctx: ResultContext[HandlerState, T, R, E]
                if self._has_result:
                    result = _CustomResult(handler_ctx, handler_state)
                    result._span = span
                    result_ctx = result
                else:
                    result_ctx = cast(ResultContext, _NOOP_RESULT_CONTEXT)
                sid = stream_id_from_context()
                if sid is None:
                    sid = new_stream_id()
                    with_stream_id(sid)
                if result is not None and self._pending is not None:
                    self._pending.set(sid, result)
                    ep.on_pending_add(sid)

                try:
                    await self._handler.consume_message(
                        handler_ctx, self._sc, handler_state, value, result_ctx)
                except Exception as err:
                    span_error(span, err)
                    span_event(span, "consume_message.error", string_attr("error", str(err)))
                    end_err = err
                    if self._has_result and self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                        await self._handler.end_request(
                            handler_ctx, self._sc, err, handler_state
                        )
                    else:
                        await self._handler.end_request(
                            handler_ctx, self._sc, err, handler_state
                        )
                    return
                span_event(span, "consume_message")

                if self._has_result:
                    if result is None:
                        raise RuntimeError("Local source result context was not initialized")
                    try:
                        time_left = ctx.time_left
                        await asyncio.wait_for(result._done, timeout=time_left)
                        span_event(span, "done_received")
                    except (asyncio.TimeoutError, TypeError):
                        end_err = TimeoutError("result wait timeout")
                        span_error(span, end_err)
                        if self._pending is not None:
                            self._pending.pop(sid)
                            ep.on_pending_remove(sid)
                        await self._handler.end_request(
                            handler_ctx, self._sc, end_err, handler_state
                        )
                        return
                    if self._pending is not None:
                        self._pending.pop(sid)
                        ep.on_pending_remove(sid)
                    await self._handler.end_request(
                        handler_ctx, self._sc, None, handler_state
                    )
                else:
                    await self._handler.end_request(
                        handler_ctx, self._sc, None, handler_state
                    )
        finally:
            ep.on_request_end(start_time, end_err)
            span.end()

    async def consume(self, value: T) -> None:
        """Entry point from data producer — applies concurrency control."""
        if self._stopped:
            return
        limit = self._handler.concurrency(self._sc)
        if limit == 0:
            task = asyncio.create_task(self._endpoint_request(value))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        else:
            if self._semaphore is None or self._concurrency_limit != limit:
                self._semaphore = asyncio.Semaphore(limit)
                self._concurrency_limit = limit
            await self._semaphore.acquire()
            semaphore = self._semaphore

            async def _run() -> None:
                try:
                    await self._endpoint_request(value)
                finally:
                    semaphore.release()

            task = asyncio.create_task(_run())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _runner(self, ctx: Context) -> None:
        await self._data_producer.start(ctx, self)

    async def start(self, ctx: Context) -> None:
        self._ctx = ctx
        if self._has_result:
            self._pending = RotatingMap[str, Any](30.0)
            await self._pending.start(ctx)
        self._runner_task = asyncio.create_task(self._runner(ctx))

    async def stop(self, ctx: Context) -> None:
        self._stopped = True
        gather_tasks: list[Any] = []
        if self._runner_task is not None:
            gather_tasks.append(self._data_producer.stop(ctx))
            gather_tasks.append(asyncio.shield(self._runner_task))
        if self._tasks:
            gather_tasks.extend(self._tasks)
        if gather_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*gather_tasks, return_exceptions=True),
                    timeout=ctx.time_left,
                )
            except (asyncio.TimeoutError, TypeError):
                self._input_stream.environment.log.warn(
                    f"Custom data source endpoint '{self.endpoint.name}' "
                    f"for stream '{self._input_stream.name}' stopped by timeout."
                )
        if self._pending is not None:
            await self._pending.stop(ctx)

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

        message_id = self._handler.get_message_id(
            result._handler_ctx, self._sc, result._handler_state, value
        )
        callback = result._message_callbacks.get(message_id)
        if callback is None:
            ep.on_unknown_message_id(sid, message_id)
            span_event(
                result._span, "unknown_message_id",
                string_attr("message_id", message_id),
            )
            return
        remove = callback(
            result._handler_ctx, self._sc, result._handler_state, value
        )
        if remove and result._message_callbacks.pop(message_id, None) is None:
            ep.on_duplicate_message_id(sid, message_id)
            span_event(
                result._span, "duplicate_message_id",
                string_attr("message_id", message_id),
            )
        span_event(
            result._span, "result_consumed",
            string_attr("message_id", message_id),
        )

    @classmethod
    def _get_or_create_datasource(
            cls, id_connector: int, env: ServiceExecutionEnvironment) -> DataSource:
        datasource = env.get_datasource(id_connector)
        if datasource is not None:
            return datasource
        cfg = env.config.get_data_connector_by_id(id_connector)
        custom_datasource = CustomDataSource(cfg.id, env)
        env.add_datasource(custom_datasource)
        return custom_datasource

    @classmethod
    def _get_or_create_endpoint(
            cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> InputEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        datasource = cls._get_or_create_datasource(cfg.id_data_connector, env)
        endpoint = datasource.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        custom_endpoint = CustomInputEndpoint(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_endpoint(custom_endpoint)
        return custom_endpoint

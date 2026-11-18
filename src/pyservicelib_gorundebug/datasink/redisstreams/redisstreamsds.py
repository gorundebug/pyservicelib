# ruff: noqa: BLE001, G010
# Native/user callback boundaries report arbitrary failures; runtime Logger uses warn().
"""Bounded native asynchronous publication using official redis-py."""

from __future__ import annotations

from ...runtime.brokermetrics import PublicationMetrics

import asyncio
import inspect
from collections.abc import Callable
from typing import Protocol

from ...runtime.common import (
    Consumer,
    OutputEndpointConsumer,
    Stream,
    TypedSinkStream,
    TypedSinkStreamWithResult,
)
from ...runtime.context.request import (
    new_stream_id,
    request_context_error,
    request_stream_id,
    stream_id_from_context,
)
from ...runtime.datasink import DataSinkEndpoint, OutputDataSink
from ...runtime.redisstreams import (
    BYTE_LIMIT,
    AdmissionRejected,
    Delivery,
    Session,
    TransportStopped,
    observe,
    snapshot,
    wait_context,
)


class SinkMessage[R]:
    def __init__(self, endpoint, collect):
        self.fields = []
        self.endpoint, self.collect = endpoint, collect
        self.used = False
        self._active = True

    def _send(self, callback):
        try:
            if not self._active:
                raise AdmissionRejected("RedisStreams publication request has ended")
            if self.used:
                raise ValueError("RedisStreams message can be sent once")
            self.used = True
            return self.endpoint.submit(self.fields, callback, self.collect)
        except Exception:
            self.endpoint.publication.observe("not-sent")
            raise


    def send(self, on_delivery: Callable[[Delivery], R]):
        """Bounded native asyncio admission, not a synchronous broker confirmation."""
        self._send(on_delivery)

    async def send_sync(self) -> Delivery:
        # Admission errors still raise. Once admitted, cancellation cannot prove withdrawal.
        admitted = self._send(None)
        try:
            return await wait_context(admitted, owned=False)
        except (Exception, asyncio.CancelledError):
            return Delivery(
                "unknown",
                TransportStopped("RedisStreams admitted publication wait interrupted"),
            )

    async def out(self, result: R):
        await self.collect(result)

    async def skip(self, result: R):
        if self.used:
            raise ValueError("RedisStreams message already sent")
        self.used = True
        await self.out(result)


class EndpointHandler[S, T, R](Protocol):
    def get_stream_id(self, value: T) -> str: ...
    def begin_request(self, stream: Stream) -> S: ...
    async def consume_message(
        self, stream: Stream, state: S, value: T, message: SinkMessage[R]
    ) -> None: ...
    async def end_request(
        self, stream: Stream, error: Exception | None, state: S
    ) -> None: ...


class RedisStreamsDataSink(OutputDataSink):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.started = self.stopping = False

    async def start(self, ctx):
        if self.started or self.stopping:
            raise TransportStopped("RedisStreams sink already started/stopped")
        self.started = True
        try:
            for ep in self.endpoints:
                if ep.config.enabled:
                    await ep.session.start(ctx)
                    await ep.start_endpoint_consumers(ctx)
                    ep.accepting = True
        except BaseException:
            await self.stop(ctx)
            raise

    async def stop(self, ctx):
        self.stopping = True
        for ep in self.endpoints:
            ep.accepting = False
        try:
            for ep in self.endpoints:
                while ep.requests or ep.deliveries:
                    await wait_context(asyncio.sleep(0.005), ctx)
        except (Exception, asyncio.CancelledError) as error:
            self.environment.log.warn(f"RedisStreams sink drain unfinished: {error}")
        finally:
            for ep in self.endpoints:
                await ep.session.close(ctx)
                for task in ep.deliveries:
                    task.cancel()
                for task in ep.request_tasks:
                    if task is not asyncio.current_task():
                        task.cancel()
                for consumer in ep.endpoint_consumers:
                    await consumer.stop(ctx)


class _Endpoint(DataSinkEndpoint):
    def __init__(self, ds, endpoint_id):
        super().__init__(ds, endpoint_id)
        self.publication = PublicationMetrics(self)
        self.accepting = False
        self.requests = 0
        self.request_tasks = set()
        self.deliveries = set()
        self.bytes = 0
        self.session = Session(
            ds.data_connector, self.config, source=False, report=self.report
        )

    async def report(self, error):
        self.environment.log.warn(f"RedisStreams sink transport: {error}")

    def submit(self, message, callback, collect):
        if not self.requests or len(self.deliveries) >= self.session.limit:
            raise AdmissionRejected(
                "RedisStreams delivery capacity exhausted or outside admitted request"
            )
        if request_context_error():
            raise AdmissionRejected("RedisStreams request cancelled before admission")
        fields, size = snapshot(message)
        if self.bytes + size > BYTE_LIMIT:
            raise AdmissionRejected("RedisStreams delivery byte capacity exhausted")
        native = observe(asyncio.create_task(self.session.publish(fields)))
        self.bytes += size

        async def finish():
            try:
                try:
                    delivery = await wait_context(native, owned=False)
                except (Exception, asyncio.CancelledError) as error:
                    delivery = Delivery(
                        "unknown",
                        (
                            error
                            if isinstance(error, Exception)
                            else TransportStopped("publication cancelled")
                        ),
                    )
                self.publication.observe(delivery.outcome)
                if callback:
                    value = callback(delivery)
                    if inspect.isawaitable(value):
                        value = await value
                    await collect(value)
                return delivery
            except Exception as error:
                await self.report(error)
                raise
            finally:
                self.bytes -= size

        task = asyncio.create_task(finish())
        self.deliveries.add(task)

        def completed(t):
            self.deliveries.discard(t)
            if not t.cancelled():
                t.exception()  # async callback failures are reported once above

        task.add_done_callback(completed)
        return task


class RedisStreamsEndpointConsumer[S, T, R](Consumer[T], OutputEndpointConsumer):
    def __init__(self, endpoint, stream, handler, collect):
        self._endpoint, self.stream, self.handler, self.collect = (
            endpoint,
            stream,
            handler,
            collect,
        )
        self.enabled = False
        endpoint.add_endpoint_consumer(self)
        stream.set_sink_consumer(self)

    @property
    def endpoint(self):
        return self._endpoint

    async def start(self, ctx):
        self.enabled = bool(self.endpoint.config.enabled)

    async def stop(self, ctx):
        self.enabled = False

    async def consume(self, value: T):
        if self.endpoint.requests >= self.endpoint.session.limit:
            raise AdmissionRejected("RedisStreams handler admission exhausted")
        token = request_stream_id.set(
            stream_id_from_context()
            or self.handler.get_stream_id(value)
            or new_stream_id()
        )
        try:
            task = asyncio.current_task()
            self.endpoint.request_tasks.add(task)
            self.endpoint.requests += 1
            try:
                await self._consume(value)
            finally:
                self.endpoint.requests -= 1
                self.endpoint.request_tasks.discard(task)
        finally:
            request_stream_id.reset(token)

    async def _consume(self, value: T):
        if not self.enabled or not self.endpoint.accepting:
            raise AdmissionRejected("RedisStreams sink endpoint is disabled or stopped")
        ep = self.endpoint
        try:
            state = self.handler.begin_request(self.stream)
            if inspect.isawaitable(state):
                state = await state
        except Exception as begin_error:
            ep.on_begin_request_failed(begin_error)
            return
        started = ep.on_request_start()
        error = None
        cancelled = False
        message = SinkMessage(ep, self.collect)
        try:
            await self.handler.consume_message(self.stream, state, value, message)
            if request_context_error():
                raise TransportStopped("RedisStreams sink request context cancelled")
        except (Exception, asyncio.CancelledError) as err:
            cancelled = isinstance(err, asyncio.CancelledError)
            error = (
                err
                if isinstance(err, Exception)
                else TransportStopped("RedisStreams sink request cancelled")
            )
        finally:
            message._active = False
            try:
                await self.handler.end_request(self.stream, error, state)
            finally:
                ep.on_request_end(started, error)
        if cancelled:
            raise asyncio.CancelledError


def _make(stream, handler, collect):
    env = stream.environment
    cfg = env.config.get_endpoint_config_by_id(stream.endpoint_id)
    ds = env.get_datasink(cfg.id_data_connector)
    if ds is None:
        ds = RedisStreamsDataSink(cfg.id_data_connector, env)
        env.add_datasink(ds)
    if not isinstance(ds, RedisStreamsDataSink):
        raise TypeError("RedisStreams sink connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return RedisStreamsEndpointConsumer(ep, stream, handler, collect)


def make_redisstreams_endpoint_consumer[S, T, E](
    stream: TypedSinkStream[T, E], handler: EndpointHandler[S, T, E]
) -> Consumer[T]:
    return _make(stream, handler, stream.error_stream.consume)


def make_redisstreams_endpoint_consumer_with_result[S, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E], handler: EndpointHandler[S, T, R]
) -> Consumer[T]:
    return _make(stream, handler, stream.consume_result)

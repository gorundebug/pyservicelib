"""Bounded native NATS publishing with independent request and delivery lifetimes."""
from __future__ import annotations

from ...runtime.brokermetrics import PublicationMetrics

import asyncio
import inspect
from contextvars import copy_context
from dataclasses import dataclass
from typing import Callable, Protocol, Any, Iterable, cast

from nats.js.api import PubAck

from ...runtime.common import Consumer, OutputEndpointConsumer, TypedSinkStream, TypedSinkStreamWithResult, Stream
from ...runtime.context import Context
from ...runtime.context.request import request_context_error, request_stream_id, stream_id_from_context, new_stream_id
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint
from ...runtime.nats import (
    Connection, AdmissionRejected, TransportStopped, QUEUE_LIMIT, MAX_MESSAGE_BYTES, BYTE_LIMIT,
    validate_endpoint, ensure_resources, wait_context, outgoing_headers,
)


@dataclass(frozen=True)
class Publication:
    """Core 'accepted'/'flushed' never means durable ACK or subscriber processing."""
    kind: str
    ack: PubAck | None = None


@dataclass(frozen=True)
class Delivery:
    publication: Publication | None
    error: Exception | None


class SinkMessage[R]:
    def __init__(self, endpoint, collect):
        self.subject = endpoint.config.subject
        self.data = b""
        self.headers: dict[str, str] = {}
        self._endpoint, self._collect = endpoint, collect
        self._used = False

    def _send(self, callback, *, sync):
        try:
            if self._used:
                raise ValueError("NATS SinkMessage may publish at most once")
            if not isinstance(self.data, bytes) or len(self.data) > MAX_MESSAGE_BYTES:
                raise ValueError("NATS payload must be bytes of at most 1 MiB")
            # Keep endpoint routing explicit; do not publish into a different stream.
            if self.subject != self._endpoint.config.subject:
                raise ValueError("NATS publish subject must match the modeled endpoint")
            headers = outgoing_headers(self.headers)
            if any(not isinstance(k, str) or not isinstance(v, str) or
                   any(c in k + v for c in '\r\n') for k, v in headers.items()):
                raise ValueError("NATS headers must be strings without CR/LF")
            if sum(len(k.encode()) + len(v.encode()) for k, v in headers.items()) > 65536:
                raise ValueError("NATS headers exceed 64 KiB")
            done = self._endpoint.datasink.admit(self._endpoint, self.data, headers,
                                                callback, self._collect, sync=sync)
            self._used = True
            return done
        except Exception:
            self._endpoint.publication.observe("error")
            raise


    def send(self, on_delivery: Callable[[Delivery], R]) -> None:
        """Bounded local admission; callback follows native publication outcome."""
        self._send(on_delivery, sync=False)

    async def send_sync(self) -> Delivery:
        """Wait on the same native path; Core additionally flushes. No auto out()."""
        future = self._send(None, sync=True)
        return await wait_context(future, owned=False)

    async def out(self, result: R):
        await self._collect(result)

    async def skip(self, result: R):
        if self._used:
            raise ValueError("NATS message already published")
        self._used = True
        await self.out(result)


class EndpointHandler[S, T, R](Protocol):
    def get_stream_id(self, value: T) -> str: ...
    def begin_request(self, stream: Stream) -> S: ...
    async def consume_message(self, stream: Stream, state: S, value: T, message: SinkMessage[R]) -> None: ...
    async def end_request(self, stream: Stream, error: Exception | None, state: S) -> None: ...


class NatsDataSink(OutputDataSink):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.connection = Connection(self.data_connector, self._report)
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.workers = []
        self.accepting = False
        self.started = False
        self.owner = None
        self._stop_task = None
        self._start_task = None
        self.cleanup_context = None

    async def _report(self, error):
        self.environment.log.warn(f"NATS sink transport error: {error}")

    @property
    def endpoints(self) -> Iterable[_Endpoint]:
        return cast(Iterable[_Endpoint], super().endpoints)

    async def start(self, ctx: Context):
        if self._start_task is not None or self._stop_task is not None:
            raise TransportStopped("NATS connector already started or stopped")
        self._start_task = asyncio.create_task(self._start(ctx))
        await wait_context(self._start_task, ctx)

    async def _start(self, ctx: Context):
        if self.started or self._stop_task or ctx.is_expired:
            raise TransportStopped("NATS sink already started, stopped or cancelled")
        self.started = True
        self.owner = ctx.child()
        try:
            for ep in self.endpoints:
                if ep.config.enabled:
                    validate_endpoint(ep.config, jetstream=self.connection.jetstream, source=False)
            await self.connection.start(ctx)
            for ep in self.endpoints:
                if ep.config.enabled and self.connection.js:
                    await wait_context(ensure_resources(self.connection.js, ep.config, source=False, connection=self.connection), ctx)
            self.workers = [asyncio.create_task(self._worker()) for _ in range(4)]
            self.accepting = True
            for ep in self.endpoints:
                await ep.start_endpoint_consumers(ctx)
        except BaseException as startup_error:
            self.accepting = False
            for worker in self.workers:
                worker.cancel()
            await asyncio.gather(*self.workers, return_exceptions=True)
            try:
                await self.connection.close()
            except BaseException as cleanup_error:
                startup_error.add_note(f"NATS startup cleanup failed: {cleanup_error}")
            raise

    def admit(self, endpoint, data, headers, callback, collect, *, sync):
        error = request_context_error()
        if error:
            raise AdmissionRejected(f"NATS publication not admitted: {error}")
        if (not self.accepting or self.owner is None or self.owner.is_expired or not self.connection.nc.is_connected):
            raise AdmissionRejected("NATS publication not admitted: stopped or reconnecting")
        future = asyncio.get_running_loop().create_future()
        item = (endpoint, data, headers, callback, collect, sync, future, copy_context())
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            raise AdmissionRejected("NATS publication queue full (64); publication not admitted") from None
        return future

    async def _worker(self):
        while True:
            item = await self.queue.get()
            try:
                if item is None:
                    return
                # The admitted delivery retains caller ContextVars even after EndRequest.
                await asyncio.create_task(self._deliver(item), context=item[-1])
            finally:
                if item is not None and not item[6].done():
                    # Cancellation can precede the delivery coroutine's first step.
                    item[0].publication.observe("error")
                    item[6].set_result(Delivery(None, AdmissionRejected("NATS worker stopped before transmission")))
                    await self._report(AdmissionRejected("NATS admitted publication not transmitted"))
                self.queue.task_done()

    async def _deliver(self, item):
        ep, data, headers, callback, collect, sync, future, _ = item
        delivery = None
        native = None
        cancelled = False
        try:
            # Cancellation BEFORE transmission is unsent. Once transmitted,
            # caller cancellation cannot withdraw delivery or erase its outcome.
            if error := request_context_error():
                raise AdmissionRejected(f"NATS admitted publication cancelled before transmission: {error}")
            if not self.connection.nc.is_connected:
                raise AdmissionRejected("NATS disconnected before transmission")
            if self.connection.nc.pending_data_size + len(data) + sum(len(k.encode()) + len(v.encode()) for k, v in headers.items()) > BYTE_LIMIT:
                raise AdmissionRejected("NATS native pending buffer full before transmission")
            if self.connection.js:
                native = await self.connection.js.publish_async(
                    ep.config.subject, data, headers=headers, stream=ep.config.stream_name,
                    wait_stall=self.connection.timeout)
                ack = await asyncio.wait_for(native, self.connection.timeout)
                delivery = Delivery(Publication("jetstream", ack), None)
            else:
                await asyncio.wait_for(self.connection.nc.publish(ep.config.subject, data, headers=headers),
                                       self.connection.timeout)
                if sync:
                    await self.connection.flush()
                delivery = Delivery(Publication("flushed" if sync else "accepted"), None)
        except (Exception, asyncio.CancelledError) as error:
            cancelled = isinstance(error, asyncio.CancelledError)
            if cancelled:
                error = TransportStopped("NATS drain deadline: publication outcome may be unknown")
            delivery = Delivery(None, error if isinstance(error, Exception) else TransportStopped("NATS publication cancelled"))
        finally:
            if native is not None and not native.done():
                # Official future cancellation frees the SDK's pending semaphore.
                native.cancel()
            ep.publication.observe({"jetstream": "acknowledged", "flushed": "flushed", "accepted": "client-accepted"}.get(delivery.publication.kind, "error") if delivery.publication and delivery.error is None else "error")
            if not future.done():
                future.set_result(delivery)
        try:
            if callback is not None:
                value = callback(delivery)
                if inspect.isawaitable(value):
                    value = await value
                await wait_context(collect(value), self.cleanup_context or Context(),
                                   timeout=self.connection.timeout)
            elif delivery.error:
                await self._report(delivery.error)
        except (Exception, asyncio.CancelledError) as error:
            await self._report(RuntimeError(f"NATS delivery result callback unfinished: {error}"))
        if cancelled:
            raise asyncio.CancelledError

    async def stop(self, ctx: Context):
        self.accepting = False
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop(ctx))
        await asyncio.shield(self._stop_task)

    async def _stop(self, ctx):
        self.cleanup_context = ctx
        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
        try:
            await wait_context(self.queue.join(), ctx)
            for _ in self.workers:
                await self.queue.put(None)
            await wait_context(asyncio.gather(*self.workers), ctx)
            await wait_context(self.connection.flush(), ctx)
        except (Exception, asyncio.CancelledError) as error:
            await self._report(RuntimeError(f"NATS sink shutdown unfinished: {error}"))
        finally:
            for worker in self.workers:
                worker.cancel()
            await asyncio.gather(*self.workers, return_exceptions=True)
            # No success is synthesized for admitted work that never started.
            while not self.queue.empty():
                item = self.queue.get_nowait()
                self.queue.task_done()
                if item is not None:
                    _, _, _, _, _, _, future, _ = item
                    if not future.done():
                        item[0].publication.observe("error")
                        future.set_result(Delivery(None, AdmissionRejected("NATS stopped before transmission")))
                    await self._report(AdmissionRejected("NATS admitted publication unfinished before transmission"))
            try:
                await self.connection.close(ctx)
            except Exception as error:
                self.environment.log.warn(f"NATS native close unfinished at shutdown deadline: {error}")
            for ep in self.endpoints:
                await ep.stop_endpoint_consumers(ctx)


class _Endpoint(DataSinkEndpoint):
    def __init__(self, ds, endpoint_id):
        super().__init__(ds, endpoint_id)
        self.publication = PublicationMetrics(self)

    async def start(self, ctx):
        await self.start_endpoint_consumers(ctx)

    async def stop(self, ctx):
        await self.stop_endpoint_consumers(ctx)


class NatsEndpointConsumer[S, T, R](Consumer[T], OutputEndpointConsumer):
    def __init__(self, endpoint, stream, handler, collect):
        self._endpoint, self.stream, self.handler, self.collect = endpoint, stream, handler, collect
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
        token = request_stream_id.set(stream_id_from_context() or self.handler.get_stream_id(value) or new_stream_id())
        try:
            await self._consume(value)
        finally:
            request_stream_id.reset(token)

    async def _consume(self, value: T):
        if not self.enabled:
            raise AdmissionRejected("NATS sink endpoint is disabled or stopped")
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
        try:
            await self.handler.consume_message(self.stream, state, value, SinkMessage(ep, self.collect))
        except (Exception, asyncio.CancelledError) as err:
            cancelled = isinstance(err, asyncio.CancelledError)
            error = err if isinstance(err, Exception) else TransportStopped("NATS sink request cancelled")
        finally:
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
        ds = NatsDataSink(cfg.id_data_connector, env)
        env.add_datasink(ds)
    if not isinstance(ds, NatsDataSink):
        raise TypeError("NATS sink connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return NatsEndpointConsumer(ep, stream, handler, collect)


def make_nats_endpoint_consumer[S, T, E](stream: TypedSinkStream[T, E],
                                        handler: EndpointHandler[S, T, E]) -> Consumer[T]:
    return _make(stream, handler, stream.error_stream.consume)


def make_nats_endpoint_consumer_with_result[S, T, R, E](stream: TypedSinkStreamWithResult[T, R, E],
                                                       handler: EndpointHandler[S, T, R]) -> Consumer[T]:
    return _make(stream, handler, stream.consume_result)

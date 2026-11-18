"""Typed native Core NATS and durable pull JetStream sources (official nats.py)."""
from __future__ import annotations
from ...runtime.brokermetrics import SourceMetrics

import asyncio
import inspect
from typing import Protocol, Callable, Any, Iterable, cast

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError, SlowConsumerError

from ...runtime.common import Consumer, StreamContext, CollectFunc, TypedInputStream
from ...runtime.context import Context
from ...runtime.context.request import request_context_scope, new_stream_id, with_stream_id, stream_id_from_context
from ...runtime.datasource import InputDataSource, DataSourceEndpoint, DataSourceEndpointConsumer
from ...runtime.nats import (
    Connection, TransportStopped, QUEUE_LIMIT, BYTE_LIMIT, MAX_MESSAGE_BYTES,
    ensure_resources, validate_endpoint, wait_context, incoming_headers,
)


class ConsumerMessage:
    """Core message. Deliberately has no ACK/NAK or durable metadata API."""
    def __init__(self, native: Msg):
        self.data = native.data
        self.subject = native.subject
        self.reply = native.reply
        self.headers = dict(native.headers or {})


class JetStreamMessage(ConsumerMessage):
    """Native settlement belongs to this delivery and active connection epoch."""
    def __init__(self, native: Msg, connection: Connection):
        super().__init__(native)
        self._native, self._connection = native, connection
        self._epoch = connection.epoch
        self._active = True
        self.metadata = native.metadata

    def _check(self):
        if (not self._active or self._connection.closed or not self._connection.nc.is_connected
                or self._connection.epoch != self._epoch):
            raise TransportStopped("JetStream settlement handle is expired or disconnected")

    async def ack(self):
        """Request native ACK; does not wait for server confirmation."""
        self._check()
        await self._native.ack()

    async def ack_sync(self):
        """Wait for the native server ACK response (not business durability)."""
        self._check()
        await self._native.ack_sync(timeout=self._connection.timeout)

    async def nak(self, delay: float | None = None):
        self._check()
        await self._native.nak(delay=delay)

    async def in_progress(self):
        self._check()
        await self._native.in_progress()

    async def term(self):
        self._check()
        await self._native.term()


class ResultContext[S, T, R, E]:
    """Local completion only. cancel(error) propagates an asynchronous cause."""
    def __init__(self, state: S, owner: Context):
        self.state, self.owner = state, owner
        self.callbacks: dict[str, Callable] = {}
        self.completion = asyncio.get_running_loop().create_future()
        self.error: Exception | None = None
        self.active = True
        self.lock = asyncio.Lock()

    def set_result_callback(self, message_id: str, callback: Callable):
        if not self.active:
            raise TransportStopped("NATS request is complete")
        if len(self.callbacks) >= QUEUE_LIMIT and message_id not in self.callbacks:
            raise ValueError("NATS result callback limit exceeded")
        self.callbacks[message_id] = callback

    def done(self):
        if self.active and not self.completion.done():
            self.completion.set_result(None)

    def cancel(self, error: Exception):
        if self.active and not self.completion.done():
            self.error = error
            self.owner.cancel()


class EndpointHandler[S, T, R, E](Protocol):
    def concurrency(self, sc: StreamContext[T, R, E]) -> int: ...
    async def on_error(self, sc: StreamContext[T, R, E], error: Exception) -> None: ...
    async def begin_request(self, sc: StreamContext[T, R, E]) -> S: ...
    async def consume_message(self, sc: StreamContext[T, R, E], state: S,
                              msg: ConsumerMessage | JetStreamMessage,
                              result: ResultContext[S, T, R, E]) -> None: ...
    def get_message_id(self, sc: StreamContext[T, R, E], state: S, value: R) -> str: ...
    async def end_request(self, sc: StreamContext[T, R, E], error: Exception | None, state: S) -> None: ...


class NatsDataSource(InputDataSource):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.connection = Connection(self.data_connector, self._report)
        self.connection.on_disconnect = self._disconnected
        self.connection.on_reconnect = self._reconnected
        self.owner = None
        self.started = False
        self.stopping = False
        self._stop_task = None
        self._start_task = None

    async def _disconnected(self):
        for ep in self.endpoints:
            if ep.consumer:
                ep.consumer.disconnected()

    async def _reconnected(self):
        if self.stopping:
            return
        for ep in self.endpoints:
            if ep.config.enabled:
                await ep.consumer.resume()

    async def _report(self, error):
        for ep in self.endpoints:
            if ep.consumer is not None:
                await ep.consumer.report(error)
        if isinstance(error, SlowConsumerError):
            # Core cannot backpressure a remote publisher: overflow is a fatal,
            # observable loss, never successful processing or an ACK.
            for ep in self.endpoints:
                if ep.consumer is not None:
                    ep.consumer.stop_admission()
                    if ep.consumer.subscription:
                        await ep.consumer.subscription.unsubscribe()

    @property
    def endpoints(self) -> Iterable[_Endpoint]:
        return cast(Iterable[_Endpoint], super().endpoints)

    async def start(self, ctx: Context):
        if self._start_task is not None or self._stop_task is not None:
            raise TransportStopped("NATS connector already started or stopped")
        self._start_task = asyncio.create_task(self._start(ctx))
        await wait_context(self._start_task, ctx)

    async def _start(self, ctx: Context):
        if self.started or self.stopping or ctx.is_expired:
            raise TransportStopped("NATS source is already started, stopped or cancelled")
        self.started = True
        self.owner = ctx.child()
        try:
            for ep in self.endpoints:
                if ep.config.enabled:
                    validate_endpoint(ep.config, jetstream=self.connection.jetstream, source=True)
                    ep.consumer.validate()
            await self.connection.start(self.owner)
            # Validate/provision every resource before any input is admitted.
            if self.connection.js:
                for ep in self.endpoints:
                    if ep.config.enabled:
                        await wait_context(ensure_resources(self.connection.js, ep.config, source=True, connection=self.connection), self.owner)
            for ep in self.endpoints:
                if ep.config.enabled:
                    await ep.consumer.start(self.owner)
            await wait_context(self.connection.flush(), self.owner)
        except BaseException as startup_error:
            self.stopping = True
            for ep in self.endpoints:
                if ep.consumer:
                    await ep.consumer.abort()
            try:
                await self.connection.close()
            except BaseException as cleanup_error:
                startup_error.add_note(f"NATS startup cleanup failed: {cleanup_error}")
            raise

    async def stop(self, ctx: Context):
        if self._stop_task is None:
            self.stopping = True
            for ep in self.endpoints:
                if ep.consumer:
                    ep.consumer.stop_admission()
            self._stop_task = asyncio.create_task(self._stop(ctx))
        await asyncio.shield(self._stop_task)

    async def _stop(self, ctx):
        for ep in self.endpoints:
            if ep.consumer:
                ep.consumer.cleanup_context = ctx
        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
        try:
            for ep in self.endpoints:
                if ep.consumer:
                    await wait_context(ep.consumer.drain(), ctx)
            await wait_context(self.connection.flush(), ctx)
        except (Exception, asyncio.CancelledError) as error:
            self.environment.log.warn(f"NATS source shutdown unfinished: {error}")
        finally:
            if self.owner:
                self.owner.cancel()
            for ep in self.endpoints:
                if ep.consumer:
                    await ep.consumer.abort()
            try:
                await self.connection.close(ctx)
            except Exception as error:
                self.environment.log.warn(f"NATS native close unfinished at shutdown deadline: {error}")


class _Endpoint(DataSourceEndpoint):
    consumer: Any = None

    async def start(self, ctx):
        if self.consumer:
            await self.consumer.start(ctx)

    async def stop(self, ctx):
        if self.consumer:
            self.consumer.stop_admission()
            await wait_context(self.consumer.drain(), ctx)


class _ResultConsumer[R](Consumer[R]):
    def __init__(self, target):
        self.target = target

    async def consume(self, value: R):
        await self.target.consume_result(value)


class NatsEndpointConsumer[S, T, R, E](DataSourceEndpointConsumer[T, R, E]):
    def __init__(self, endpoint, stream, handler):
        super().__init__(endpoint, stream)
        self.handler, self.ep = handler, endpoint
        self.observations = SourceMetrics(endpoint)
        self.sc = StreamContext(stream, stream.get_result_stream(), CollectFunc(stream.consume),
                                CollectFunc(stream.error_stream.consume))
        self.has_result = stream.get_result_stream() is not None
        self.pending: dict[str, ResultContext] = {}
        self.active_owners: set[Context] = set()
        self.admission_closed = False
        self.cleanup_context = None
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.workers = []
        self.pump = None
        self.subscription: Any = None
        self.owner = None
        self.accepting = False
        if self.has_result:
            stream.set_result_consumer(_ResultConsumer(self))
        if endpoint.consumer is not None:
            raise ValueError("NATS source endpoint has one typed handler; use separate endpoints for independent flows")
        endpoint.consumer = self
        endpoint.add_endpoint_consumer(self)

    @property
    def connection(self):
        return self.ep.datasource.connection

    def validate(self):
        limit = self.handler.concurrency(self.sc)
        if type(limit) is not int or not 1 <= limit <= QUEUE_LIMIT:
            raise ValueError("NATS source concurrency must be between 1 and 64")
        self.limit = limit

    async def report(self, error):
        try:
            if self.owner:
                with request_context_scope(self.owner):
                    await asyncio.wait_for(self.handler.on_error(self.sc, error), self.connection.timeout)
            else:
                await asyncio.wait_for(self.handler.on_error(self.sc, error), self.connection.timeout)
        except Exception as callback_error:
            self.ep.environment.log.warn(f"NATS on_error failed: {callback_error}")

    async def start(self, ctx):
        self.validate()
        self.owner = ctx.child()
        await self._subscribe()
        self.accepting = True
        self.workers = [asyncio.create_task(self._worker()) for _ in range(self.limit)]
        self.pump = asyncio.create_task(self._receive())

    async def _subscribe(self):
        cfg = self.ep.config
        if self.connection.js:
            self.subscription = await self.connection.js.pull_subscribe_bind(
                consumer=cfg.durable_name, stream=cfg.stream_name,
                pending_msgs_limit=QUEUE_LIMIT, pending_bytes_limit=BYTE_LIMIT)
        else:
            self.subscription = await self.connection.nc.subscribe(
                cfg.subject, queue=cfg.queue_group or "",
                pending_msgs_limit=QUEUE_LIMIT, pending_bytes_limit=BYTE_LIMIT)

    def disconnected(self):
        if self.pump:
            self.pump.cancel()
        for owner in self.active_owners:
            owner.cancel()

    async def resume(self):
        if self.admission_closed:
            return
        if self.pump:
            await asyncio.gather(self.pump, return_exceptions=True)
        if self.subscription:
            await self.subscription.unsubscribe()
        if self.connection.js:
            cfg = self.ep.config.model_copy(update={"create_stream": False, "create_consumer": False})
            await ensure_resources(self.connection.js, cfg, source=True, connection=self.connection)
        await self._subscribe()
        self.pump = asyncio.create_task(self._receive())

    async def _receive(self):
        try:
            while self.accepting:
                await wait_context(self.connection.ready.wait(), self.owner)
                try:
                    if self.connection.js:
                        messages = await wait_context(self.subscription.fetch(1, timeout=0.5), self.owner)
                        native = messages[0]
                    else:
                        native = await wait_context(self.subscription.next_msg(timeout=0.5), self.owner)
                except (NatsTimeoutError, TimeoutError):
                    continue
                except Exception as error:
                    await self.report(error)
                    if self.connection.nc.is_closed:
                        return
                    await asyncio.sleep(0.1)
                    continue
                if len(native.data) > MAX_MESSAGE_BYTES or sum(len(k.encode()) + len(v.encode()) for k, v in (native.headers or {}).items()) > 65536:
                    await self.report(ValueError("NATS message exceeds payload/header profile limits; not acknowledged"))
                    self.accepting = False
                    return
                await wait_context(self.queue.put((native, self.connection.epoch)), self.owner)
        except (TransportStopped, asyncio.CancelledError):
            pass
        except Exception as error:
            await self.report(error)

    async def _worker(self):
        assert self.owner is not None
        while True:
            item = await self.queue.get()
            try:
                if item is None:
                    return
                native, epoch = item
                if epoch != self.connection.epoch:
                    await self.report(TransportStopped("NATS queued delivery expired during reconnect"))
                    continue
                with request_context_scope(self.owner.child()):
                    # Separate task preserves source correlation from subsequent messages.
                    await asyncio.create_task(self._request(native))
                if (task := asyncio.current_task()) is not None and task.cancelling():
                    return
            finally:
                self.queue.task_done()

    async def _request(self, native):
        self.observations.observe("received")
        if self.observations.counters and self.observations.protocol == "jetstream":
            if native.metadata.num_delivered > 1:
                self.observations.observe("redelivered")
        with incoming_headers(native.headers) as carrier:
            await self._request_inner(native, carrier)

    async def _request_inner(self, native, carrier):
        assert self.owner is not None
        owner = self.owner.child()
        self.active_owners.add(owner)
        sid = carrier.get("x-stream-id") or new_stream_id()
        if sid in self.pending:
            self.active_owners.discard(owner)
            await self.report(ValueError("Duplicate active NATS stream ID; delivery not admitted"))
            return
        msg = JetStreamMessage(native, self.connection) if self.connection.js else ConsumerMessage(native)
        state = None
        begun = False
        result = None
        error = None
        started = None
        with request_context_scope(owner):
            with_stream_id(sid)
            try:
                try:
                    state = await wait_context(self.handler.begin_request(self.sc), owner)
                    begun = True
                except Exception as err:
                    self.ep.on_begin_request_failed(err)
                    await self.report(err)
                    return
                started = self.ep.on_request_start()
                result = ResultContext(state, owner)
                if self.has_result:
                    self.pending[sid] = result
                    self.ep.on_pending_add(sid)
                await wait_context(self.handler.consume_message(self.sc, state, msg, result), owner)
                if self.has_result:
                    await wait_context(result.completion, owner, owned=False)
            except (Exception, asyncio.CancelledError) as err:
                error = err if isinstance(err, Exception) else TransportStopped("NATS request cancelled")
            finally:
                if result:
                    # Synchronize with a running callback before completion wins over cancellation.
                    async with result.lock:
                        if result.completion.done() and error is not None and isinstance(error, (TransportStopped, TimeoutError)):
                            error = None
                        elif result.error:
                            error = result.error
                        result.active = False
                        result.callbacks.clear()
                        if self.pending.pop(sid, None) is not None:
                            self.ep.on_pending_remove(sid)
                try:
                    if begun:
                        await wait_context(self.handler.end_request(self.sc, error, state),
                            self.cleanup_context or Context(), timeout=self.connection.timeout)
                except Exception as err:
                    if self.cleanup_context and self.cleanup_context.is_expired:
                        self.ep.environment.log.warn(f"NATS EndRequest unfinished at shutdown deadline: {err}")
                    else:
                        await self.report(err)
                finally:
                    if isinstance(msg, JetStreamMessage):
                        msg._active = False
                    owner.cancel()
                    self.active_owners.discard(owner)
                    if started is not None:
                        self.ep.on_request_end(started, error)

    async def consume_result(self, value: R):
        sid = stream_id_from_context()
        result = self.pending.get(sid or "")
        if result is None:
            self.ep.on_late_result(sid or "")
            return
        async with result.lock:
            if not result.active:
                return
            key = self.handler.get_message_id(self.sc, result.state, value)
            callback = result.callbacks.get(key)
            if callback is None:
                self.ep.on_unknown_message_id(sid, key)
                return
            try:
                remove = callback(self.sc, result.state, value)
                if inspect.isawaitable(remove):
                    remove = await wait_context(remove, result.owner, timeout=self.connection.timeout)
                if remove:
                    result.callbacks.pop(key, None)
            except (Exception, asyncio.CancelledError) as error:
                result.cancel(error if isinstance(error, Exception) else TransportStopped("NATS result callback cancelled"))

    def stop_admission(self):
        self.admission_closed = True
        self.accepting = False
        if self.pump:
            self.pump.cancel()

    async def drain(self):
        if self.pump:
            await asyncio.gather(self.pump, return_exceptions=True)
        if self.subscription:
            await self.subscription.unsubscribe()
        await self.queue.join()
        for _ in self.workers:
            await self.queue.put(None)
        await asyncio.gather(*self.workers)

    async def abort(self):
        self.stop_admission()
        if self.owner:
            self.owner.cancel()
        tasks = ([self.pump] if self.pump else []) + self.workers
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()


def make_nats_endpoint_consumer[S, T, R, E](stream: TypedInputStream[T, R, E],
                                            handler: EndpointHandler[S, T, R, E]) -> Consumer[T]:
    env = stream.environment
    cfg = env.config.get_endpoint_config_by_id(stream.endpoint_id)
    ds = env.get_datasource(cfg.id_data_connector)
    if ds is None:
        ds = NatsDataSource(cfg.id_data_connector, env)
        env.add_datasource(ds)
    if not isinstance(ds, NatsDataSource):
        raise TypeError("NATS source connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return NatsEndpointConsumer(ep, stream, handler)

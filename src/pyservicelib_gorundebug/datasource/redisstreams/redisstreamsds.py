# ruff: noqa: BLE001, G010
# Native/user callback boundaries report arbitrary failures; runtime Logger uses warn().
"""Typed Redis Streams source with explicit XACK and bounded concurrent processing."""

from __future__ import annotations
from ...runtime.brokermetrics import SourceMetrics

import asyncio
import inspect
import time
from collections.abc import Callable
from contextvars import ContextVar
from typing import Protocol

from ...runtime.common import CollectFunc, Consumer, StreamContext, TypedInputStream
from ...runtime.context import Context
from ...runtime.context.request import (
    new_stream_id,
    request_context_scope,
    stream_id_from_context,
    with_stream_id,
)
from ...runtime.datasource import (
    DataSourceEndpoint,
    DataSourceEndpointConsumer,
    InputDataSource,
)
from ...runtime.redisstreams import (
    BYTE_LIMIT,
    MAX_MESSAGE_BYTES,
    QUEUE_LIMIT,
    DeletedPendingEntriesError,
    Entry,
    Session,
    TransportStopped,
    incoming_headers,
    metadata,
    observe,
    wait_context,
)

_request_identity = ContextVar("redisstreams_request_identity", default=None)


class ConsumerMessage:
    def __init__(self, entry, epoch, session):
        self.message: Entry = entry
        self.data = entry.fields
        self.headers = metadata(entry)
        self.epoch, self.session = epoch, session
        self._active = True
        self._used = False
        self.owner = None

    async def ack(self) -> int:
        """Explicit XACK count. Done never calls this, and XACK does not delete data."""
        if not self._active or self._used or (self.owner and self.owner.is_expired):
            raise TransportStopped(
                "RedisStreams delivery request is complete or cancelled"
            )
        self._used = True
        return await self.session.ack(self.message.id, self.epoch, self.owner)


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
            raise TransportStopped("RedisStreams request is complete")
        if len(self.callbacks) >= QUEUE_LIMIT and message_id not in self.callbacks:
            raise ValueError("RedisStreams result callback limit exceeded")
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
    async def consume_message(
        self,
        sc: StreamContext[T, R, E],
        state: S,
        msg: ConsumerMessage,
        result: ResultContext[S, T, R, E],
    ) -> None: ...
    def get_message_id(self, sc: StreamContext[T, R, E], state: S, value: R) -> str: ...
    async def end_request(
        self, sc: StreamContext[T, R, E], error: Exception | None, state: S
    ) -> None: ...


class RedisStreamsDataSource(InputDataSource):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.started = False
        self.stopping = False
        self.owner = None

    async def start(self, ctx):
        if self.started or self.stopping:
            raise TransportStopped("RedisStreams source already started/stopped")
        self.started = True
        self.owner = ctx.child()
        try:
            for ep in self.endpoints:
                if ep.config.enabled:
                    await ep.consumer.start(self.owner)
        except BaseException:
            await self.stop(ctx)
            raise

    async def stop(self, ctx):
        self.stopping = True
        for ep in self.endpoints:
            if ep.consumer:
                ep.consumer.stop_admission()
        try:
            for ep in self.endpoints:
                if ep.consumer:
                    ep.consumer.cleanup_context = ctx
                    await wait_context(ep.consumer.drain(), ctx)
        except (Exception, asyncio.CancelledError) as error:
            self.environment.log.warn(f"RedisStreams source drain unfinished: {error}")
        finally:
            if self.owner:
                self.owner.cancel()
            for ep in self.endpoints:
                if ep.consumer:
                    await ep.consumer.abort(ctx)


class _Endpoint(DataSourceEndpoint):
    consumer = None

    async def start(self, ctx):
        await self.consumer.start(ctx)

    async def stop(self, ctx):
        self.consumer.stop_admission()
        await wait_context(self.consumer.drain(), ctx)


class _ResultConsumer(Consumer):
    def __init__(self, target):
        self.target = target

    async def consume(self, value):
        await self.target.consume_result(value)


class RedisStreamsEndpointConsumer[S, T, R, E](DataSourceEndpointConsumer[T, R, E]):
    def __init__(self, endpoint, stream, handler):
        super().__init__(endpoint, stream)
        self.handler, self.ep = handler, endpoint
        self.observations = SourceMetrics(endpoint)
        self.sc = StreamContext(
            stream,
            stream.get_result_stream(),
            CollectFunc(stream.consume),
            CollectFunc(stream.error_stream.consume),
        )
        self.has_result = stream.get_result_stream() is not None
        self.pending = {}
        self.active_owners = set()
        self.cleanup_context = None
        self.workers = set()
        self.reader = None
        self.bytes = 0
        self.owner = None
        self.accepting = False
        self.connection = Session(
            endpoint.datasource.data_connector,
            endpoint.config,
            source=True,
            report=self.disconnected,
        )
        if endpoint.consumer is not None:
            raise ValueError("RedisStreams source endpoint has one typed handler")
        endpoint.consumer = self
        endpoint.add_endpoint_consumer(self)
        if self.has_result:
            stream.set_result_consumer(_ResultConsumer(self))

    async def disconnected(self, error):
        for owner in self.active_owners:
            owner.cancel()
        await self.report(error)

    async def report(self, error):
        try:
            await self.handler.on_error(self.sc, error)
        except Exception as callback_error:
            self.ep.environment.log.warn(
                f"RedisStreams on_error failed: {callback_error}"
            )

    async def start(self, ctx):
        count = self.handler.concurrency(self.sc)
        if type(count) is not int or not 1 <= count <= min(64, self.connection.limit):
            raise ValueError("RedisStreams concurrency must be 1..min(64, readCount)")
        self.concurrency = count
        self.owner = ctx.child()
        await self.connection.start(ctx)
        self.accepting = True
        self.reader = observe(asyncio.create_task(self._read_loop()))

    async def _read_loop(self):
        cursor, next_claim = "0-0", 0.0
        while (
            self.accepting and not self.owner.is_expired and not self.connection.stopped
        ):
            try:
                if not self.connection.ready:
                    await asyncio.sleep(0.025)
                    continue
                free = min(
                    self.concurrency - len(self.workers),
                    (BYTE_LIMIT - self.bytes) // MAX_MESSAGE_BYTES,
                )
                if free < 1:
                    await asyncio.wait(
                        self.workers, return_when=asyncio.FIRST_COMPLETED
                    )
                    continue
                epoch = self.connection.epoch
                if time.monotonic() >= next_claim:
                    cursor, batch, deleted = await self.connection.claim(
                        cursor, free, self.owner
                    )
                    for _ in batch:
                        self.observations.observe("reclaimed")
                    next_claim = (
                        time.monotonic()
                        + (self.connection.ep.claim_interval_ms or 1000) / 1000
                    )
                    if deleted:
                        await self.report(DeletedPendingEntriesError(deleted))
                else:
                    block = min(
                        self.connection.ep.read_block_ms or 1000,
                        max(1, int((next_claim - time.monotonic()) * 1000)),
                    )
                    batch = await self.connection.read(free, block, self.owner)
                if not self.accepting or epoch != self.connection.epoch:
                    continue
                for entry in batch:
                    if entry.size > MAX_MESSAGE_BYTES:
                        await self.report(
                            ValueError(
                                f"RedisStreams entry {entry.id} exceeds 1 MiB; left pending"
                            )
                        )
                        continue
                    self.bytes += entry.size

                    async def handle(entry=entry, epoch=epoch):
                        try:
                            await self._request(
                                ConsumerMessage(entry, epoch, self.connection)
                            )
                        finally:
                            self.bytes -= entry.size

                    task = observe(asyncio.create_task(handle()))
                    self.workers.add(task)
                    task.add_done_callback(self.workers.discard)
            except asyncio.CancelledError:
                return
            except Exception as error:
                if not self.accepting:
                    break
                if self.connection.ready:
                    await self.report(error)
                    await asyncio.sleep(
                        (self.connection.ep.claim_interval_ms or 1000) / 1000
                    )

    async def _request(self, native):
        self.observations.observe("received")
        with incoming_headers(native.headers) as carrier:
            await self._request_inner(native, carrier)

    async def _request_inner(self, native, carrier):
        assert self.owner is not None
        owner = self.owner.child()
        self.active_owners.add(owner)
        native.owner = owner
        sid = carrier.get("x-stream-id") or new_stream_id()
        identity = new_stream_id()
        _request_identity.set(identity)
        msg = native
        state = None
        begun = False
        result = None
        error = None
        started = None
        with request_context_scope(owner):
            with_stream_id(sid)
            try:
                try:
                    state = await self.handler.begin_request(self.sc)
                    begun = True
                except Exception as err:
                    self.ep.on_begin_request_failed(err)
                    await self.report(err)
                    return
                started = self.ep.on_request_start()
                result = ResultContext(state, owner)
                if self.has_result:
                    self.pending[identity] = result
                    self.ep.on_pending_add(sid)
                await self.handler.consume_message(self.sc, state, msg, result)
                if owner.is_expired:
                    raise TransportStopped("RedisStreams request context cancelled")
                if self.has_result:
                    await wait_context(result.completion, owner, owned=False)
            except (Exception, asyncio.CancelledError) as err:
                error = (
                    err
                    if isinstance(err, Exception)
                    else TransportStopped("RedisStreams request cancelled")
                )
            finally:
                if result:
                    # Synchronize with a running callback before completion wins over cancellation.
                    async with result.lock:
                        if (
                            self.has_result
                            and result.completion.done()
                            and error is not None
                            and isinstance(error, (TransportStopped, TimeoutError))
                        ):
                            error = None
                        elif result.error:
                            error = result.error
                        result.active = False
                        result.callbacks.clear()
                        if self.pending.pop(identity, None) is not None:
                            self.ep.on_pending_remove(sid)
                try:
                    if begun:
                        await self.handler.end_request(self.sc, error, state)
                except Exception as err:
                    if self.cleanup_context and self.cleanup_context.is_expired:
                        self.ep.environment.log.warn(
                            f"RedisStreams EndRequest unfinished at shutdown deadline: {err}"
                        )
                    else:
                        await self.report(err)
                finally:
                    if isinstance(msg, ConsumerMessage):
                        msg._active = False
                    owner.cancel()
                    self.active_owners.discard(owner)
                    if started is not None:
                        self.ep.on_request_end(started, error)

    async def consume_result(self, value: R):
        sid = stream_id_from_context()
        result = self.pending.get(_request_identity.get())
        if result is None:
            self.ep.on_late_result(sid or "")
            return
        async with result.lock:
            if not result.active:
                return
            try:
                key = self.handler.get_message_id(self.sc, result.state, value)
                callback = result.callbacks.get(key)
                if callback is None:
                    self.ep.on_unknown_message_id(sid, key)
                    return
                remove = callback(self.sc, result.state, value)
                if inspect.isawaitable(remove):
                    remove = await remove
                if remove:
                    result.callbacks.pop(key, None)
            except (Exception, asyncio.CancelledError) as error:
                result.cancel(
                    error
                    if isinstance(error, Exception)
                    else TransportStopped("RedisStreams result callback cancelled")
                )

    def stop_admission(self):
        self.accepting = False
        self.connection.pause()
        if self.reader:
            self.reader.cancel()

    async def drain(self):
        if self.workers:
            await asyncio.wait(self.workers)

    async def abort(self, ctx):
        self.stop_admission()
        if self.owner:
            self.owner.cancel()
        for task in self.workers:
            task.cancel()
        await self.connection.close(ctx)


def make_redisstreams_endpoint_consumer[S, T, R, E](
    stream: TypedInputStream[T, R, E], handler: EndpointHandler[S, T, R, E]
) -> Consumer[T]:
    env = stream.environment
    cfg = env.config.get_endpoint_config_by_id(stream.endpoint_id)
    ds = env.get_datasource(cfg.id_data_connector)
    if ds is None:
        ds = RedisStreamsDataSource(cfg.id_data_connector, env)
        env.add_datasource(ds)
    if not isinstance(ds, RedisStreamsDataSource):
        raise TypeError("RedisStreams source connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return RedisStreamsEndpointConsumer(ep, stream, handler)

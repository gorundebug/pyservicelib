"""Typed RabbitMQ AMQP 1.0 source with explicit per-delivery settlement."""

from __future__ import annotations
from ...runtime.brokermetrics import SourceMetrics
import asyncio
import inspect
from typing import Protocol, Callable, Any
from rabbitmq_amqp_python_client import Message
from ...runtime.common import Consumer, StreamContext, CollectFunc, TypedInputStream
from ...runtime.context import Context
from ...runtime.context.request import (
    request_context_scope,
    new_stream_id,
    with_stream_id,
    stream_id_from_context,
)
from ...runtime.datasource import (
    InputDataSource,
    DataSourceEndpoint,
    DataSourceEndpointConsumer,
)
from ...runtime.rabbitmq import (
    Session,
    TransportStopped,
    QUEUE_LIMIT,
    wait_context,
    incoming_headers,
)


class ConsumerMessage:
    def __init__(self, encoded, serial, epoch, session):
        self.message = Message()
        self.message.decode(encoded)
        self.data = self.message.body
        self.headers = self.message.application_properties or {}
        self.serial, self.epoch, self.session = serial, epoch, session
        self._active = True
        self.owner = None

    async def _settle(self, action):
        if not self._active or self.owner is not None and self.owner.is_expired:
            raise TransportStopped("RabbitMQ delivery request is complete or cancelled")
        await self.session.settle(self.serial, self.epoch, action)

    async def accept(self):
        await self._settle("accept")

    async def release(self):
        await self._settle("requeue")

    async def reject(self):
        await self._settle("discard")


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
            raise TransportStopped("RabbitMQ request is complete")
        if len(self.callbacks) >= QUEUE_LIMIT and message_id not in self.callbacks:
            raise ValueError("RabbitMQ result callback limit exceeded")
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


class RabbitMQDataSource(InputDataSource):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.started = False
        self.stopping = False
        self.owner = None

    async def start(self, ctx):
        if self.started or self.stopping:
            raise TransportStopped("RabbitMQ source already started/stopped")
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
                    await wait_context(ep.consumer.drain(), ctx, owned=False)
        except (Exception, asyncio.CancelledError) as error:
            self.environment.log.warn(f"RabbitMQ source drain unfinished: {error}")
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


class RabbitMQEndpointConsumer[S, T, R, E](DataSourceEndpointConsumer[T, R, E]):
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
        self.workers = []
        self.owner = None
        self.accepting = False
        self.connection = Session(
            endpoint.datasource.data_connector,
            endpoint.config,
            source=True,
            report=self.disconnected,
        )
        if endpoint.consumer is not None:
            raise ValueError("RabbitMQ source endpoint has one typed handler")
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
            await wait_context(
                self.handler.on_error(self.sc, error), timeout=self.connection.timeout
            )
        except Exception as callback_error:
            self.ep.environment.log.warn(f"RabbitMQ on_error failed: {callback_error}")

    async def start(self, ctx):
        count = self.handler.concurrency(self.sc)
        if type(count) is not int or not 1 <= count <= min(64, self.connection.limit):
            raise ValueError(
                "RabbitMQ source concurrency must be 1..min(64, consumerCredits)"
            )
        self.owner = ctx.child()
        await self.connection.start(ctx)
        self.accepting = True
        self.workers = [asyncio.create_task(self._worker()) for _ in range(count)]

    async def _worker(self):
        while self.accepting:
            try:
                encoded, serial, epoch = await asyncio.wait_for(
                    self.connection.messages.get(), 0.05
                )
            except TimeoutError:
                continue
            if not self.accepting:
                break
            if epoch != self.connection.epoch:
                continue
            msg = ConsumerMessage(encoded, serial, epoch, self.connection)
            with request_context_scope(self.owner.child()):
                await self._request(msg)

    async def _request(self, native):
        self.observations.observe("received")
        if self.observations.counters and native.message.delivery_count > 0:
            self.observations.observe("redelivered")
        with incoming_headers(native.message.application_properties) as carrier:
            await self._request_inner(native, carrier)

    async def _request_inner(self, native, carrier):
        assert self.owner is not None
        owner = self.owner.child()
        self.active_owners.add(owner)
        native.owner = owner
        sid = carrier.get("x-stream-id") or new_stream_id()
        if sid in self.pending:
            self.active_owners.discard(owner)
            await self.report(
                ValueError("Duplicate active RabbitMQ stream ID; delivery not admitted")
            )
            return
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
                    state = await wait_context(
                        self.handler.begin_request(self.sc), owner
                    )
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
                await wait_context(
                    self.handler.consume_message(self.sc, state, msg, result), owner
                )
                if self.has_result:
                    await wait_context(result.completion, owner, owned=False)
            except (Exception, asyncio.CancelledError) as err:
                error = (
                    err
                    if isinstance(err, Exception)
                    else TransportStopped("RabbitMQ request cancelled")
                )
            finally:
                if result:
                    # Synchronize with a running callback before completion wins over cancellation.
                    async with result.lock:
                        if (
                            result.completion.done()
                            and error is not None
                            and isinstance(error, (TransportStopped, TimeoutError))
                        ):
                            error = None
                        elif result.error:
                            error = result.error
                        result.active = False
                        result.callbacks.clear()
                        if self.pending.pop(sid, None) is not None:
                            self.ep.on_pending_remove(sid)
                try:
                    if begun:
                        await wait_context(
                            self.handler.end_request(self.sc, error, state),
                            self.cleanup_context or Context(),
                            timeout=self.connection.timeout,
                        )
                except Exception as err:
                    if self.cleanup_context and self.cleanup_context.is_expired:
                        self.ep.environment.log.warn(
                            f"RabbitMQ EndRequest unfinished at shutdown deadline: {err}"
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
                    remove = await wait_context(
                        remove, result.owner, timeout=self.connection.timeout
                    )
                if remove:
                    result.callbacks.pop(key, None)
            except (Exception, asyncio.CancelledError) as error:
                result.cancel(
                    error
                    if isinstance(error, Exception)
                    else TransportStopped("RabbitMQ result callback cancelled")
                )

    def stop_admission(self):
        self.accepting = False
        self.connection.pause()

    async def drain(self):
        if self.workers:
            await asyncio.gather(*self.workers)

    async def abort(self, ctx):
        self.stop_admission()
        if self.owner:
            self.owner.cancel()
        for task in self.workers:
            task.cancel()
        await self.connection.close(ctx)


def make_rabbitmq_endpoint_consumer[S, T, R, E](
    stream: TypedInputStream[T, R, E], handler: EndpointHandler[S, T, R, E]
) -> Consumer[T]:
    env = stream.environment
    cfg = env.config.get_endpoint_config_by_id(stream.endpoint_id)
    ds = env.get_datasource(cfg.id_data_connector)
    if ds is None:
        ds = RabbitMQDataSource(cfg.id_data_connector, env)
        env.add_datasource(ds)
    if not isinstance(ds, RabbitMQDataSource):
        raise TypeError("RabbitMQ source connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return RabbitMQEndpointConsumer(ep, stream, handler)

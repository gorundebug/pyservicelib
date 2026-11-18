"""Bounded asynchronous admission to the official RabbitMQ SDK owner."""

from __future__ import annotations

from ...runtime.brokermetrics import PublicationMetrics
import asyncio
import inspect
from typing import Protocol, Callable
from rabbitmq_amqp_python_client import Message
from ...runtime.common import (
    Consumer,
    OutputEndpointConsumer,
    TypedSinkStream,
    TypedSinkStreamWithResult,
    Stream,
)
from ...runtime.context import Context
from ...runtime.context.request import (
    request_context_error,
    request_stream_id,
    stream_id_from_context,
    new_stream_id,
)
from ...runtime.datasink import OutputDataSink, DataSinkEndpoint
from ...runtime.rabbitmq import (
    Session,
    Delivery,
    AdmissionRejected,
    TransportStopped,
    wait_context,
    BYTE_LIMIT,
)


class SinkMessage[R]:
    def __init__(self, endpoint, collect):
        self.message = Message(body=b"", inferred=True)
        self.endpoint, self.collect = endpoint, collect
        self.used = False

    def _send(self, callback):
        try:
            if self.used:
                raise ValueError("RabbitMQ message can be sent once")
            self.used = True
            return self.endpoint.submit(self.message, callback, self.collect)
        except Exception:
            self.endpoint.publication.observe("not-sent")
            raise


    def send(self, on_delivery: Callable[[Delivery], R]):
        """Bounded worker admission, not a synchronous broker confirmation."""
        self._send(on_delivery)

    async def send_sync(self) -> Delivery:
        return await wait_context(self._send(None), owned=False)

    async def out(self, result: R):
        await self.collect(result)

    async def skip(self, result: R):
        if self.used:
            raise ValueError("RabbitMQ message already sent")
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


class RabbitMQDataSink(OutputDataSink):
    def __init__(self, connector_id, env):
        super().__init__(connector_id, env)
        self.started = self.stopping = False

    async def start(self, ctx):
        if self.started or self.stopping:
            raise TransportStopped("RabbitMQ sink already started/stopped")
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
            self.environment.log.warn(f"RabbitMQ sink drain unfinished: {error}")
        finally:
            for ep in self.endpoints:
                await ep.session.close(ctx)
                for consumer in ep.endpoint_consumers:
                    await consumer.stop(ctx)


class _Endpoint(DataSinkEndpoint):
    def __init__(self, ds, endpoint_id):
        super().__init__(ds, endpoint_id)
        self.publication = PublicationMetrics(self)
        self.accepting = False
        self.requests = 0
        self.deliveries = set()
        self.bytes = 0
        self.session = Session(
            ds.data_connector, self.config, source=False, report=self.report
        )

    async def report(self, error):
        self.environment.log.warn(f"RabbitMQ sink transport: {error}")

    def submit(self, message, callback, collect):
        if not self.requests or len(self.deliveries) >= self.session.limit:
            raise AdmissionRejected(
                "RabbitMQ delivery capacity exhausted or outside admitted request"
            )
        size = len(message.encode())
        if self.bytes + size > BYTE_LIMIT:
            raise AdmissionRejected("RabbitMQ delivery byte capacity exhausted")
        native = self.session.publish(message)
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


class RabbitMQEndpointConsumer[S, T, R](Consumer[T], OutputEndpointConsumer):
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
        token = request_stream_id.set(
            stream_id_from_context()
            or self.handler.get_stream_id(value)
            or new_stream_id()
        )
        try:
            self.endpoint.requests += 1
            try:
                await self._consume(value)
            finally:
                self.endpoint.requests -= 1
        finally:
            request_stream_id.reset(token)

    async def _consume(self, value: T):
        if not self.enabled or not self.endpoint.accepting:
            raise AdmissionRejected("RabbitMQ sink endpoint is disabled or stopped")
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
            await self.handler.consume_message(
                self.stream, state, value, SinkMessage(ep, self.collect)
            )
        except (Exception, asyncio.CancelledError) as err:
            cancelled = isinstance(err, asyncio.CancelledError)
            error = (
                err
                if isinstance(err, Exception)
                else TransportStopped("RabbitMQ sink request cancelled")
            )
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
        ds = RabbitMQDataSink(cfg.id_data_connector, env)
        env.add_datasink(ds)
    if not isinstance(ds, RabbitMQDataSink):
        raise TypeError("RabbitMQ sink connector already has a different adapter")
    ep = ds.get_endpoint(stream.endpoint_id)
    if ep is None:
        ep = _Endpoint(ds, stream.endpoint_id)
        ds.add_endpoint(ep)
    return RabbitMQEndpointConsumer(ep, stream, handler, collect)


def make_rabbitmq_endpoint_consumer[S, T, E](
    stream: TypedSinkStream[T, E], handler: EndpointHandler[S, T, E]
) -> Consumer[T]:
    return _make(stream, handler, stream.error_stream.consume)


def make_rabbitmq_endpoint_consumer_with_result[S, T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E], handler: EndpointHandler[S, T, R]
) -> Consumer[T]:
    return _make(stream, handler, stream.consume_result)

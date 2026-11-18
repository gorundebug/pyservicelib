# ruff: noqa: BLE001
"""Official pulsar.asyncio transport. No custom callback bridge or executor."""

from __future__ import annotations

import asyncio
import math
import ssl
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import quote

import aiohttp
import pulsar
import pulsar.asyncio as native
from opentelemetry import context as otel_context
from opentelemetry import propagate

from .config.pulsar import PulsarDataConnectorConfig, PulsarEndpointConfig
from .context.request import (
    request_cancelled,
    request_context_error,
    request_deadline,
    stream_id_from_context,
)
from .environment.tracing import (
    sampling_enabled,
    sampling_requested_by_carrier,
    sampling_scope,
)

BYTE_LIMIT = 8 * 1024 * 1024
MAX_MESSAGE_BYTES = 1024 * 1024
QUEUE_LIMIT = 4096
_incoming_trace_marker = ContextVar("pulsar_trace_marker", default=None)


class TransportStopped(RuntimeError):
    pass


class AdmissionRejected(RuntimeError):
    outcome = "NotSent"


@dataclass(frozen=True)
class Delivery:
    outcome: str
    error: BaseException | None = None
    id: pulsar.MessageId | None = None


@dataclass
class ProducerMessage:
    data: bytes = b""
    properties: dict[str, str] = field(default_factory=dict)
    key: str | None = None
    event_timestamp: int | None = None


def observe(task):
    def done(t):
        if not t.cancelled():
            t.exception()

    task.add_done_callback(done)
    return task


async def wait_context(operation, owner=None, *, timeout=None, owned=True):
    """Bound waiting without awaiting cancellation-resistant user code."""
    task = observe(asyncio.ensure_future(operation))
    event = asyncio.Event() if owner is not None else request_cancelled.get()
    loop = asyncio.get_running_loop()
    remove = (
        owner.add_cancel_callback(lambda: loop.call_soon_threadsafe(event.set))
        if owner is not None
        else lambda: None
    )
    remaining = owner.time_left if owner else None
    if owner is None and (deadline := request_deadline.get()) is not None:
        remaining = max(0.0, (deadline - datetime.now(UTC)).total_seconds())
    if remaining is not None:
        timeout = remaining if timeout is None else min(timeout, remaining)
    cancel = asyncio.create_task(event.wait()) if event is not None else None
    try:
        if (owner and owner.is_expired) or (event and event.is_set()):
            raise TransportStopped("Pulsar context cancelled")
        done, _ = await asyncio.wait(
            [task] + ([cancel] if cancel else []),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            return task.result()
        if cancel in done:
            raise TransportStopped("Pulsar context cancelled")
        raise TimeoutError("Pulsar deadline exceeded; outcome may be unknown")
    finally:
        remove()
        if cancel:
            cancel.cancel()
        if owned and not task.done():
            task.cancel()


@contextmanager
def incoming_headers(headers):
    carrier = {
        key.lower(): value
        for key, value in (headers or {}).items()
        if isinstance(key, str) and isinstance(value, str)
    }
    token = otel_context.attach(propagate.extract(carrier))
    marker = _incoming_trace_marker.set(carrier.get("x-trace"))
    try:
        with sampling_scope(sampling_requested_by_carrier(carrier)):
            yield carrier
    finally:
        otel_context.detach(token)
        _incoming_trace_marker.reset(marker)


def outgoing_headers(headers):
    result = dict(headers)
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    sid = stream_id_from_context()
    if sid:
        carrier["x-stream-id"] = sid
    if sampling_enabled():
        carrier["x-trace"] = "1"
    elif _incoming_trace_marker.get():
        carrier["x-trace"] = _incoming_trace_marker.get() or ""
    for key, value in carrier.items():
        for existing in list(result):
            if existing.lower() == key.lower():
                del result[existing]
        result[key] = value
    return result


def snapshot(message):
    if not isinstance(message, ProducerMessage) or not isinstance(
        message.data, (bytes, bytearray, memoryview)
    ):
        raise AdmissionRejected("Pulsar requires ProducerMessage with binary data")
    if len(message.properties) > QUEUE_LIMIT or any(
        not isinstance(k, str) or not isinstance(v, str)
        for k, v in message.properties.items()
    ):
        raise AdmissionRejected("Pulsar properties must be bounded string pairs")
    if message.key is not None and not isinstance(message.key, str):
        raise AdmissionRejected("Pulsar partition key must be a string")
    if message.event_timestamp is not None and (
        type(message.event_timestamp) is not int
        or not 0 <= message.event_timestamp < 2**63
    ):
        raise AdmissionRejected(
            "Pulsar event timestamp must be nonnegative milliseconds"
        )
    props = outgoing_headers(message.properties)
    size = (
        len(message.data)
        + sum(len(k.encode()) + len(v.encode()) + 32 for k, v in props.items())
        + len((message.key or "").encode())
    )
    if size > MAX_MESSAGE_BYTES:
        raise AdmissionRejected("Pulsar message exceeds 1 MiB")
    return ProducerMessage(
        bytes(message.data), props, message.key, message.event_timestamp
    ), size


class AdminError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"Pulsar administration returned HTTP {status}")


async def ensure_resources(cfg, ep, source):
    tls = ssl.create_default_context(cafile=cfg.tls_ca_file or None)
    if cfg.tls_cert_file:
        tls.load_cert_chain(cfg.tls_cert_file, cfg.tls_key_file)
    timeout = aiohttp.ClientTimeout(
        total=cfg.operation_timeout, connect=cfg.connect_timeout
    )
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    admin_token = cfg.admin_token or cfg.token
    if admin_token:
        headers["Authorization"] = "Bearer " + admin_token
    async with aiohttp.ClientSession(
        timeout=timeout, headers=headers, connector=aiohttp.TCPConnector(ssl=tls)
    ) as http:

        async def request(method, path, body=None):
            async with http.request(
                method, cfg.admin_url + path, json=body, allow_redirects=False
            ) as response:
                if not 200 <= response.status < 300:
                    raise AdminError(response.status)
                chunks = []
                size = 0
                async for chunk in response.content.iter_chunked(65536):
                    size += len(chunk)
                    if size > 4 << 20:
                        raise ValueError("Pulsar admin response exceeds limit")
                    chunks.append(chunk)
                data = b"".join(chunks)
                if not data:
                    return None
                import json

                return json.loads(data)

        parts = [
            quote(p, safe="") for p in ep.topic.removeprefix("persistent://").split("/")
        ]
        ns = "/admin/v2/namespaces/" + "/".join(parts[:2])
        topic = "/admin/v2/persistent/" + "/".join(parts)
        policies = [("autoTopicCreation", "allowAutoTopicCreation")]
        if source:
            policies.append(
                ("autoSubscriptionCreation", "allowAutoSubscriptionCreation")
            )
        for policy, key in policies:
            value = await request("GET", ns + "/" + policy)
            if not isinstance(value, dict) or value.get(key) is not False:
                raise ValueError("Pulsar namespace must explicitly disable " + key)

        async def partitions():
            value = await request(
                "GET", topic + "/partitions?checkAllowAutoCreation=false"
            )
            n = value.get("partitions") if isinstance(value, dict) else None
            if type(n) is not int or n < 0:
                raise ValueError("Invalid Pulsar partition metadata")
            if n == 0:
                await request("GET", topic + "/stats")
            return n

        try:
            count = await partitions()
        except AdminError as error:
            if error.status != 404 or not ep.create_topic:
                raise
            try:
                await request(
                    "PUT",
                    topic + ("/partitions" if ep.partitions else ""),
                    ep.partitions if ep.partitions else None,
                )
            except AdminError as conflict:
                if conflict.status != 409:
                    raise
            count = await partitions()
        if ep.create_topic and ep.partitions != count:
            raise ValueError(
                "Existing Pulsar partition count differs; no reconciliation"
            )
        if not source:
            return
        subscriptions = await request("GET", topic + "/subscriptions")
        if ep.subscription_name in subscriptions:
            return
        if not ep.create_subscription:
            raise ValueError("Pulsar subscription missing; creation disabled")
        position = (2**63 - 1) if ep.subscription_initial_position == "Latest" else -1
        try:
            await request(
                "PUT",
                topic + "/subscription/" + quote(ep.subscription_name, safe=""),
                {"ledgerId": position, "entryId": position, "partitionIndex": -1},
            )
        except AdminError as error:
            if error.status != 409:
                raise
        if ep.subscription_name not in await request("GET", topic + "/subscriptions"):
            raise ValueError("Pulsar subscription not visible after creation")


def delivery_error(error):
    if isinstance(error, native.PulsarException):
        code = error.error()
        if code in (
            pulsar.Result.ProducerQueueIsFull,
            pulsar.Result.MemoryBufferIsFull,
            pulsar.Result.InvalidMessage,
            pulsar.Result.MessageTooBig,
            pulsar.Result.ProducerNotInitialized,
        ):
            return Delivery("NotSent", error)
        if code in (
            pulsar.Result.AuthorizationError,
            pulsar.Result.AuthenticationError,
            pulsar.Result.TopicTerminated,
            pulsar.Result.ProducerFenced,
            pulsar.Result.IncompatibleSchema,
            pulsar.Result.NotAllowedError,
        ):
            return Delivery("Rejected", error)
    return Delivery("Unknown", error)


class Session:
    def __init__(self, cfg, ep, *, source, report):
        self.cfg = PulsarDataConnectorConfig.model_validate(
            cfg.model_dump(exclude_unset=True) | {"adminToken": cfg.admin_token}
        )
        self.ep = PulsarEndpointConfig.model_validate(
            ep.model_dump(exclude_unset=True)
        ).validate_role(source)
        self.source, self.report = source, report
        self.limit = (
            (self.ep.receiver_queue_size or 256)
            if source
            else (self.ep.publisher_max_in_flight or 256)
        )
        self.client = self.consumer = self.producer = None
        self.stopped = False
        self.ready = False
        self.epoch = 0
        self.active = set()
        self.close_task = None
        self.receiving = False

    async def start(self, ctx):
        if self.client is not None or self.stopped:
            raise TransportStopped("Pulsar session already started/stopped")
        await wait_context(
            ensure_resources(self.cfg, self.ep, self.source),
            ctx,
            timeout=self.cfg.operation_timeout,
        )
        cfg = self.cfg
        auth = pulsar.AuthenticationToken(cfg.token) if cfg.token else None
        if cfg.tls_cert_file:
            auth = pulsar.AuthenticationTLS(cfg.tls_cert_file, cfg.tls_key_file)
        self.client = native.Client(
            cfg.servers[0],
            authentication=auth,
            operation_timeout_seconds=max(1, math.ceil(cfg.operation_timeout)),
            connection_timeout_ms=cfg.connect_timeout_ms or 2000,
            tls_validate_hostname=True,
            tls_allow_insecure_connection=False,
            tls_trust_certs_file_path=cfg.tls_ca_file or None,
            tls_certificate_file_path=cfg.tls_cert_file or None,
            tls_private_key_file_path=cfg.tls_key_file or None,
        )
        try:
            if self.source:
                self.consumer = await wait_context(
                    self.client.subscribe(
                        self.ep.topic,
                        self.ep.subscription_name,
                        consumer_type=getattr(
                            pulsar.ConsumerType, self.ep.subscription_type or "Shared"
                        ),
                        receiver_queue_size=self.limit,
                        max_total_receiver_queue_size_across_partitions=self.limit,
                        negative_ack_redelivery_delay_ms=self.ep.negative_ack_redelivery_delay_ms
                        or 60000,
                        batch_index_ack_enabled=True,
                    ),
                    ctx,
                    timeout=cfg.operation_timeout,
                )
            else:
                self.producer = await wait_context(
                    self.client.create_producer(
                        self.ep.topic,
                        batching_enabled=False,
                        block_if_queue_full=False,
                        max_pending_messages=self.limit,
                        max_pending_messages_across_partitions=self.limit,
                        send_timeout_millis=cfg.operation_timeout_ms or 5000,
                    ),
                    ctx,
                    timeout=cfg.operation_timeout,
                )
            self.ready = True
            self.epoch += 1
        except BaseException:
            await self.close(ctx)
            raise

    async def receive(self, ctx):
        if not self.ready or self.stopped or self.receiving:
            raise TransportStopped("Pulsar receive unavailable")
        self.receiving = True
        try:
            return await wait_context(self.consumer.receive(), ctx)
        finally:
            self.receiving = False

    async def settle(self, message, epoch, owner, nack=False):
        if (
            self.stopped
            or not self.ready
            or epoch != self.epoch
            or (owner and owner.is_expired)
        ):
            raise TransportStopped("Pulsar delivery scope ended")
        operation = (
            self.consumer.negative_acknowledge(message)
            if nack
            else self.consumer.acknowledge(message)
        )
        await wait_context(operation, owner, timeout=self.cfg.operation_timeout)

    async def publish(self, message):
        if (
            self.source
            or self.producer is None
            or not self.ready
            or self.stopped
            or len(self.active) >= self.limit
        ):
            return Delivery(
                "NotSent", AdmissionRejected("Pulsar publication unavailable or full")
            )
        if error := request_context_error():
            return Delivery("NotSent", error)
        try:
            message, _ = snapshot(message)
        except Exception as error:
            return Delivery("NotSent", error)
        handed = False

        async def send():
            nonlocal handed
            if error := request_context_error():
                raise error
            handed = True
            return await self.producer.send(
                message.data,
                properties=message.properties,
                partition_key=message.key,
                event_timestamp=message.event_timestamp,
            )

        task = observe(asyncio.create_task(send()))
        self.active.add(task)
        task.add_done_callback(self.active.discard)
        try:
            identity = await wait_context(
                task, owned=False, timeout=self.cfg.operation_timeout
            )
            return Delivery("Accepted", id=identity)
        except (Exception, asyncio.CancelledError) as error:
            cause = error
            if not handed:
                task.cancel()
                return Delivery("NotSent", cause)
            return delivery_error(cause)

    async def close(self, ctx):
        self.stopped = True
        self.ready = False
        self.epoch += 1
        if self.client is None:
            return
        if self.close_task is None:
            self.close_task = observe(asyncio.create_task(self.client.close()))
        try:
            await wait_context(
                self.close_task, ctx, owned=False, timeout=self.cfg.operation_timeout
            )
        except (Exception, asyncio.CancelledError) as error:
            raise TransportStopped(
                "Pulsar native cleanup unfinished at stop deadline"
            ) from error

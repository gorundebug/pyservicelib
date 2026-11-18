"""Shared official nats.py boundary. No provisioning occurs outside explicit flags."""
from __future__ import annotations

import asyncio
import ssl
import json
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar
from opentelemetry import context as otel_context, propagate
from typing import Any, cast
from urllib.parse import urlsplit
from pathlib import Path

from nats.aio.client import Client
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, ReplayPolicy, StorageType, ConsumerInfo
from nats.js.errors import NotFoundError, APIError

from .context import Context
from .context.request import request_cancelled, request_deadline, stream_id_from_context
from .environment.tracing import sampling_enabled, sampling_scope, sampling_requested_by_carrier

QUEUE_LIMIT = 64
BYTE_LIMIT = 8 * 1024 * 1024
MAX_MESSAGE_BYTES = 1024 * 1024
_incoming_trace_marker: ContextVar[str | None] = ContextVar("nats_trace_marker", default=None)


class TransportStopped(RuntimeError):
    pass


class AdmissionRejected(RuntimeError):
    pass


async def wait_context(operation, owner: Context | None = None, *, timeout=None, owned=True):
    """Wait without blocking the loop; optionally leave an admitted operation owned."""
    task = asyncio.ensure_future(operation)
    event = asyncio.Event() if owner is not None else request_cancelled.get()
    remove = owner.add_cancel_callback(event.set) if owner is not None and event is not None else lambda: None
    remaining = owner.time_left if owner is not None else None
    if owner is None and (deadline := request_deadline.get()) is not None:
        remaining = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
    if remaining is not None:
        timeout = remaining if timeout is None else min(timeout, remaining)
    expires = asyncio.get_running_loop().time() + timeout if timeout is not None else None
    cancel = asyncio.create_task(event.wait()) if event is not None else None
    try:
        if event is not None and event.is_set():
            raise TransportStopped("NATS request context cancelled")
        done, _ = await asyncio.wait([task] + ([cancel] if cancel else []),
                                     timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            if (event is not None and event.is_set()) or (owner is not None and owner.cancelled):
                raise TransportStopped("NATS request context cancelled")
            if expires is not None and asyncio.get_running_loop().time() >= expires:
                raise TimeoutError("NATS operation deadline exceeded; publication may be unknown")
            return task.result()
        if cancel in done:
            raise TransportStopped("NATS request context cancelled")
        raise TimeoutError("NATS operation deadline exceeded; publication may be unknown")
    finally:
        remove()
        if cancel is not None:
            cancel.cancel()
            await asyncio.gather(cancel, return_exceptions=True)
        if owned and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        elif owned and not task.cancelled():
            task.exception()  # Observe a simultaneous failure when cancellation wins.


def is_jetstream(config) -> bool:
    kind = getattr(config.type, "value", config.type)
    if kind not in (7, 8):
        raise ValueError("NATS Python adapter requires NATS (7) or JetStream (8)")
    implementation = getattr(config, "implementation", None)
    implementation = getattr(implementation, "value", implementation)
    if implementation not in (None, "", "nats.py"):
        raise ValueError(f"Unsupported Python NATS implementation: {implementation}")
    return kind == 8


def operation_timeout(config) -> float:
    value = getattr(config, "operation_timeout_ms", None)
    value = 5000 if value is None else value
    if type(value) is not int or not 1 <= value <= 9223372036854:
        raise ValueError("operationTimeoutMs must be a positive integer")
    return value / 1000


def connection_options(config) -> dict[str, Any]:
    is_jetstream(config)
    servers = getattr(config, "servers", None)
    if not isinstance(servers, (list, tuple)) or not servers:
        raise ValueError("NATS servers must be an explicit nonempty list")
    for server in servers:
        if not isinstance(server, str) or any(c.isspace() for c in server):
            raise ValueError("Invalid NATS server URL")
        parsed = urlsplit(server)
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("NATS server port must be in 1..65535")
        if (parsed.scheme not in ("nats", "tls") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path or parsed.query or parsed.fragment):
            raise ValueError("NATS requires nats:// or tls:// URLs without embedded credentials")
    timeout = getattr(config, "connect_timeout_ms", None)
    timeout = 2000 if timeout is None else timeout
    if type(timeout) is not int or not 1 <= timeout <= 9223372036854:
        raise ValueError("connectTimeoutMs must be a positive integer")
    username, password = getattr(config, "username", None), getattr(config, "password", None)
    token, credentials = getattr(config, "token", None), getattr(config, "credentials_file", None)
    if bool(username) != bool(password) or sum(map(bool, (username, token, credentials))) > 1:
        raise ValueError("NATS auth requires username/password, token, or credentialsFile exclusively")
    result = dict(servers=list(servers), connect_timeout=timeout / 1000,
                  allow_reconnect=True, max_reconnect_attempts=60,
                  pending_size=BYTE_LIMIT, flusher_queue_size=QUEUE_LIMIT,
                  flush_timeout=operation_timeout(config),
                  drain_timeout=operation_timeout(config))
    if username:
        result.update(user=username, password=password)
    if token:
        result["token"] = token
    if credentials:
        # nats.py scans credential markers synchronously; reject malformed/huge
        # files before passing them to the official parser (never log contents).
        with Path(credentials).open("rb") as handle:
            document = handle.read(65537)
        if len(document) > 65536:
            raise ValueError("NATS credentialsFile exceeds 64 KiB")
        for marker in (b"NATS USER JWT", b"USER NKEY SEED"):
            begin, end = b"-----BEGIN " + marker + b"-----", b"------END " + marker + b"------"
            if begin not in document or end not in document or not document.split(begin, 1)[1].split(end, 1)[0].strip():
                raise ValueError("Malformed NATS credentialsFile")
        result["user_credentials"] = credentials
    tls_fields = [getattr(config, name, None) for name in
                  ("tls_server_name", "tls_ca_file", "tls_cert_file", "tls_key_file")]
    hostname, ca, cert, key = tls_fields
    tls = bool(getattr(config, "tls_enabled", False))
    if (any(tls_fields) or any(s.startswith("tls://") for s in servers)) and not tls:
        raise ValueError("NATS TLS settings require TLS enabled")
    if bool(cert) != bool(key):
        raise ValueError("NATS tlsCertFile and tlsKeyFile must be supplied together")
    if tls:
        context = ssl.create_default_context(cafile=ca or None)
        if cert:
            context.load_cert_chain(cert, key)
        result.update(tls=context, tls_handshake_first=True)
        if hostname:
            result["tls_hostname"] = hostname
    return result


def validate_subject(subject: str, *, source: bool) -> None:
    if not subject or any(c.isspace() for c in subject):
        raise ValueError("NATS subject is required and cannot contain whitespace")
    tokens = subject.split(".")
    for i, token in enumerate(tokens):
        if not token or ("*" in token and (not source or token != "*")) or (
            ">" in token and (not source or token != ">" or i != len(tokens) - 1)
        ):
            raise ValueError("Invalid NATS source/publish subject")


def covers(pattern: str, subject: str) -> bool:
    """True when a single stream subject covers the entire endpoint filter."""
    left, right = pattern.split("."), subject.split(".")
    for index, token in enumerate(left):
        if token == ">":
            return index < len(right)
        if index >= len(right) or right[index] == ">":
            return False
        if token != "*" and token != right[index]:
            return False
    return len(left) == len(right)


def validate_endpoint(config, *, jetstream: bool, source: bool):
    validate_subject(config.subject or "", source=source)
    queue = getattr(config, "queue_group", None)
    if queue and (not source or jetstream or any(c.isspace() or c in "*>" for c in queue)):
        raise ValueError("queueGroup is only a Core source queue name")
    stream, durable = getattr(config, "stream_name", None), getattr(config, "durable_name", None)
    if not jetstream:
        if stream or durable or getattr(config, "create_stream", False) or getattr(config, "create_consumer", False):
            raise ValueError("Core NATS does not support JetStream resources or settlement")
        return
    for name, value in (("streamName", stream), ("durableName", durable if source else "unused")):
        if not value or any(c.isspace() or c in ".*>/\\" for c in value):
            raise ValueError(f"JetStream {name} must be a native resource name")
    if not source and (durable or getattr(config, "create_consumer", False)):
        raise ValueError("JetStream sink cannot declare a durable consumer")


async def ensure_resources(js, config, *, source: bool, connection=None):
    try:
        info = await js.stream_info(config.stream_name)
    except NotFoundError:
        if not config.create_stream:
            raise ValueError(f"JetStream stream {config.stream_name!r} does not exist (createStream=false)")
        # add_stream is create-only at this boundary; no update_stream fallback.
        info = await js.add_stream(name=config.stream_name, subjects=[config.subject],
                                   storage=StorageType.FILE, num_replicas=1)
    if not any(covers(s, config.subject) for s in info.config.subjects or []):
        raise ValueError("Existing JetStream stream does not cover endpoint subject; left unchanged")
    if not source:
        return
    try:
        consumer = await js.consumer_info(config.stream_name, config.durable_name)
    except NotFoundError:
        if not config.create_consumer:
            raise ValueError(f"JetStream consumer {config.durable_name!r} does not exist (createConsumer=false)")
        desired = ConsumerConfig(
            durable_name=config.durable_name, filter_subject=config.subject,
            ack_policy=AckPolicy.EXPLICIT, deliver_policy=DeliverPolicy.ALL,
            replay_policy=ReplayPolicy.INSTANT,
        )
        if connection is None:
            raise ValueError("Native connection required for create-only consumer provisioning")
        version = connection.nc.connected_server_version
        if (version.major, version.minor) < (2, 11):
            raise ValueError("Python createConsumer requires NATS server >=2.11 for create-only action")
        response = await connection.nc.request(
            f"$JS.API.CONSUMER.CREATE.{config.stream_name}.{config.durable_name}",
            json.dumps({"stream_name": config.stream_name, "config": desired.as_dict(),
                        "action": "create"}).encode(), timeout=connection.timeout)
        document = json.loads(response.data)
        if "error" in document:
            APIError.from_error(document["error"])
        consumer = ConsumerInfo.from_response(document)
    cfg = consumer.config
    if (cfg.durable_name != config.durable_name or cfg.deliver_subject
            or cfg.ack_policy != AckPolicy.EXPLICIT or cfg.filter_subject != config.subject
            or getattr(cfg, "filter_subjects", None)
            or cfg.deliver_policy != DeliverPolicy.ALL or cfg.replay_policy != ReplayPolicy.INSTANT):
        raise ValueError("Existing JetStream consumer is incompatible; left unchanged")


class Connection:
    def __init__(self, config, report):
        self.config, self.report = config, report
        self.jetstream = is_jetstream(config)
        self.nc = Client()
        self._connect_started = False
        self.js = None
        self.epoch = 0
        self.closed = False
        self.on_disconnect = None
        self.on_reconnect = None
        self.ready = asyncio.Event()
        self.timeout = operation_timeout(config)

    async def start(self, ctx):
        if ctx.is_expired:
            raise TransportStopped("NATS startup cancelled or expired")
        async def disconnected():
            self.epoch += 1
            self.ready.clear()
            if self.on_disconnect:
                await self.on_disconnect()
        async def reconnected():
            try:
                if self.nc.max_payload > BYTE_LIMIT:
                    raise ValueError("NATS server max_payload exceeds the 8 MiB Python profile bound")
                if self.on_reconnect:
                    await self.on_reconnect()
                self.ready.set()
            except Exception as err:
                await self.report(err)
        async def error(err):
            await self.report(err)
        async def closed():
            self.ready.clear()
            if not self.closed:
                await self.report(TransportStopped("NATS connection closed after reconnect attempts"))
        options = connection_options(self.config)
        self._connect_started = True
        await wait_context(self.nc.connect(**options,
                           error_cb=error, disconnected_cb=disconnected,
                           reconnected_cb=reconnected, closed_cb=closed), ctx, timeout=options["connect_timeout"])
        if self.nc.max_payload > BYTE_LIMIT:
            raise ValueError("NATS server max_payload exceeds the 8 MiB Python profile bound")
        self.ready.set()
        if self.jetstream:
            self.js = self.nc.jetstream(timeout=self.timeout, publish_async_max_pending=QUEUE_LIMIT)

    async def flush(self):
        # nats.py annotates seconds as int, but its asyncio waiter supports floats.
        await self.nc.flush(timeout=cast(Any, self.timeout))

    async def close(self, ctx: Context | None = None):
        self.closed = True
        self.epoch += 1
        self.ready.clear()
        if not self._connect_started:
            return
        remaining = ctx.time_left if ctx is not None else None
        timeout = self.timeout if remaining is None else min(self.timeout, max(.001, remaining))
        await asyncio.wait_for(self.nc.close(), timeout)


@contextmanager
def incoming_headers(headers):
    carrier = {key.lower(): value for key, value in (headers or {}).items()}
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

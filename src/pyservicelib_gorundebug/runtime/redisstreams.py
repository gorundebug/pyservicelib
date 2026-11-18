# ruff: noqa: BLE001
# Native/user callback boundaries report arbitrary failures; runtime Logger uses warn().
"""Official redis-py asyncio transport; raw native records, bounded owners, no XADD replay."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import uuid4

from opentelemetry import context as otel_context
from opentelemetry import propagate
from redis.asyncio import ConnectionPool
from redis.asyncio.connection import SSLConnection
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError

from .config.redisstreams import (
    RedisStreamsDataConnectorConfig,
    RedisStreamsEndpointConfig,
)
from .context import Context
from .context.request import (
    request_cancelled,
    request_context_error,
    request_context_scope,
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
_incoming_trace_marker = ContextVar("redisstreams_trace_marker", default=None)


class TransportStopped(RuntimeError):
    pass


class AdmissionRejected(RuntimeError):
    pass


class DeletedPendingEntriesError(RuntimeError):
    def __init__(self, ids):
        self.ids = tuple(ids)
        super().__init__(
            f"RedisStreams XAUTOCLAIM removed {len(ids)} deleted PEL entries"
        )


@dataclass(frozen=True)
class Delivery:
    outcome: str
    error: Exception | None = None
    id: str | None = None


@dataclass(frozen=True)
class Entry:
    id: str
    fields: tuple[tuple[bytes, bytes], ...]
    reclaimed: bool
    size: int


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
            raise TransportStopped("RedisStreams context cancelled")
        done, _ = await asyncio.wait(
            [task] + ([cancel] if cancel else []),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            return task.result()
        if cancel in done:
            raise TransportStopped("RedisStreams context cancelled")
        raise TimeoutError("RedisStreams deadline exceeded; outcome may be unknown")
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


def _value(value):
    return getattr(value, "value", value)


def _bytes(value):
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("RedisStreams fields require string or bytes values")


def snapshot(fields):
    if not isinstance(fields, (list, tuple)) or not 1 <= len(fields) <= 4096:
        raise AdmissionRejected("RedisStreams needs 1..4096 ordered field/value pairs")
    copied = [(_bytes(k), _bytes(v)) for k, v in fields]
    for key, value in outgoing_headers({}).items():
        copied = [(k, v) for k, v in copied if k.lower() != key.encode().lower()]
        copied.append((key.encode(), value.encode()))
    size = sum(len(k) + len(v) + 32 for k, v in copied)
    if size > MAX_MESSAGE_BYTES:
        raise AdmissionRejected("RedisStreams entry exceeds 1 MiB")
    return tuple(copied), size


def entries(raw, reclaimed):
    result = []
    for identity, flat in raw:
        if flat is None:
            continue
        fields = tuple(zip(flat[::2], flat[1::2]))
        size = sum(len(k) + len(v) + 32 for k, v in fields)
        result.append(
            Entry(
                identity.decode(),
                fields if size <= MAX_MESSAGE_BYTES else (),
                reclaimed,
                size,
            )
        )
    return result


def metadata(entry):
    return {
        k.decode().lower(): v.decode(errors="replace")
        for k, v in entry.fields
        if k.lower()
        in (b"x-stream-id", b"x-trace", b"traceparent", b"tracestate", b"baggage")
    }


class Session:
    def __init__(self, cfg, ep, *, source, report):
        # Revalidate generic generated configuration without changing shared interfaces.
        self.cfg = RedisStreamsDataConnectorConfig.model_validate(
            cfg.model_dump(exclude_unset=True)
        )
        self.ep = RedisStreamsEndpointConfig.model_validate(
            ep.model_dump(exclude_unset=True)
        )
        self.ep.validate_role(source)
        self.source, self.report = source, report
        self.limit = (
            (self.ep.read_count or 64)
            if source
            else (self.ep.publisher_max_in_flight or 256)
        )
        self.timeout = self.cfg.operation_timeout_ms / 1000
        self.consumer_name = self.ep.consumer_name or f"py-{self.ep.id}-{uuid4().hex}"
        self.epoch = 0
        self.ready = self.stopped = self.paused = False
        self.control = self.reader = None
        self.connections = set()
        self.operations = set()
        self.recovery = self.reporter = None
        self.owner = None
        self.unsubscribe = lambda: None
        self.attempts = 0

    def pool(self, read=False):
        c = self.cfg
        u = urlsplit(c.servers[0])
        secure = c.tls_enabled or u.scheme == "rediss"
        options = {
            "host": u.hostname,
            "port": u.port or 6379,
            "db": c.redis_database,
            "username": c.username,
            "password": c.password,
            "protocol": 2,
            "decode_responses": False,
            "retry": Retry(NoBackoff(), 0),
            "retry_on_error": [],
            "retry_on_timeout": False,
            "health_check_interval": 0,
            "socket_connect_timeout": c.connect_timeout_ms / 1000,
            "socket_timeout": self.timeout
            + ((self.ep.read_block_ms or 1000) / 1000 if read else 0),
            "max_connections": 1 if read else self.limit + 2,
        }
        if secure:
            options.update(
                connection_class=SSLConnection,
                ssl_check_hostname=True,
                ssl_cert_reqs="required",
                ssl_ca_certs=c.tls_ca_file,
                ssl_certfile=c.tls_cert_file,
                ssl_keyfile=c.tls_key_file,
            )
        return ConnectionPool(**options)

    def reconnected(self, connection):
        self.failed()
        raise TransportStopped(
            "RedisStreams pooled connection reconnected; old epoch expired"
        )

    async def raw(self, pool, args):
        # Public low-level API avoids lossy dict response callbacks and never retries commands.
        connection = await pool.get_connection()
        if connection not in self.connections:
            # Public redis-py notification detects pool-managed idle reconnects too.
            connection.register_connect_callback(self.reconnected)
            self.connections.add(connection)
        try:
            await connection.send_command(*args)
            return await connection.read_response()
        except BaseException as error:
            if not isinstance(error, ResponseError):
                await connection.disconnect(nowait=True)
            raise
        finally:
            await pool.release(connection)

    async def direct(self, pool, args, ctx):
        return await wait_context(self.raw(pool, args), ctx, timeout=self.timeout)

    async def open(self, ctx):
        if self.stopped or self.paused:
            raise TransportStopped("RedisStreams stopped")
        self.control, self.reader = (
            self.pool(),
            self.pool(True) if self.source else None,
        )
        self.epoch += 1
        try:
            info = await self.direct(self.control, ["INFO", "server"], ctx)
            values = dict(
                line.split(b":", 1) for line in info.splitlines() if b":" in line
            )
            version = tuple(int(x) for x in values[b"redis_version"].split(b".")[:2])
            if version < (7, 2) or values.get(b"redis_mode") != b"standalone":
                raise ValueError(
                    "RedisStreams requires standalone Redis >=7.2; Cluster/Sentinel unsupported"
                )
            role = await self.direct(self.control, ["ROLE"], ctx)
            if role[0] != b"master":
                raise ValueError("RedisStreams requires primary, not replica")
            ep = self.ep
            if self.source:
                await self.direct(self.reader, ["PING"], ctx)
                if ep.create_consumer_group:
                    try:
                        await self.direct(
                            self.control,
                            [
                                "XGROUP",
                                "CREATE",
                                ep.stream_key,
                                ep.consumer_group,
                                ep.group_start_id or "0-0",
                                "MKSTREAM",
                            ],
                            ctx,
                        )
                    except ResponseError as e:
                        if not str(e).startswith("BUSYGROUP "):
                            raise
                groups = await self.direct(
                    self.control, ["XINFO", "GROUPS", ep.stream_key], ctx
                )
                if not any(
                    dict(zip(g[::2], g[1::2])).get(b"name")
                    == ep.consumer_group.encode()
                    for g in groups
                ):
                    raise ValueError(
                        "RedisStreams consumer group absent; creation is opt-in"
                    )
            else:
                kind = await self.direct(self.control, ["TYPE", ep.stream_key], ctx)
                if kind not in (b"none", b"stream"):
                    raise ValueError("RedisStreams key has incompatible type")
            if self.stopped or ctx.is_expired:
                raise TransportStopped("RedisStreams startup cancelled")
            self.ready = True
        except BaseException:
            await self.disconnect()
            raise

    async def start(self, ctx):
        if self.owner is not None or self.stopped:
            raise TransportStopped("RedisStreams already started/stopped")
        self.owner = ctx.child()
        loop = asyncio.get_running_loop()

        def cancelled():
            loop.call_soon_threadsafe(
                lambda: observe(asyncio.create_task(self.close(Context())))
            )

        self.unsubscribe = ctx.add_cancel_callback(cancelled)
        try:
            await wait_context(
                self.open(ctx), ctx, timeout=self.cfg.connect_timeout_ms / 1000
            )
        except BaseException as error:
            await self.close(Context())
            if isinstance(error, (ValueError, TransportStopped, TimeoutError)):
                raise
            raise TransportStopped(
                "RedisStreams startup failed; verify auth, TLS, database and command permissions"
            ) from None

    async def disconnect(self):
        self.ready = False
        for conn in tuple(self.connections):
            await conn.disconnect(nowait=True)
        self.connections.clear()

    def failed(self):
        if self.stopped or not self.ready:
            return
        self.ready = False
        self.epoch += 1
        if not self.reporter or self.reporter.done():
            self.reporter = observe(
                asyncio.create_task(
                    self.report(
                        TransportStopped(
                            "RedisStreams connection epoch ended; pending commands may be unknown"
                        )
                    )
                )
            )
        if not self.paused and self.recovery is None:
            self.recovery = observe(asyncio.create_task(self.recover()))

    async def recover(self):
        try:
            with request_context_scope(self.owner):
                await self.disconnect()
                while (
                    not self.stopped
                    and not self.paused
                    and not self.owner.is_expired
                    and self.attempts < 60
                ):
                    self.attempts += 1
                    await wait_context(asyncio.sleep(0.25), self.owner)
                    try:
                        await wait_context(
                            self.open(self.owner),
                            self.owner,
                            timeout=self.cfg.connect_timeout_ms / 1000,
                        )
                        return
                    except (Exception, asyncio.CancelledError):
                        await self.disconnect()
                if not self.paused:
                    await self.close(Context())
        finally:
            self.recovery = None
            # A command can fail just after open marks ready but before this task retires.
            if (
                not self.ready
                and not self.stopped
                and not self.paused
                and not self.owner.is_expired
            ):
                self.recovery = observe(asyncio.create_task(self.recover()))

    async def command(self, args, ctx=None, *, read=False, timeout=None):
        if not self.ready or self.stopped or (read and self.paused):
            raise TransportStopped("RedisStreams unavailable before admission")
        task = observe(
            asyncio.create_task(self.raw(self.reader if read else self.control, args))
        )
        self.operations.add(task)
        task.add_done_callback(self.operations.discard)
        try:
            return await wait_context(task, ctx, timeout=timeout or self.timeout)
        except ResponseError:
            raise
        except BaseException:
            if not (read and self.paused):
                self.failed()
            raise

    async def publish(self, fields):
        if not self.ready or self.stopped or request_context_error():
            return Delivery(
                "not-sent",
                TransportStopped("RedisStreams unavailable/cancelled before admission"),
            )
        try:
            args = ["XADD", self.ep.stream_key]
            if not self.ep.create_stream:
                args.append("NOMKSTREAM")
            args += ["*"] + [v for pair in fields for v in pair]
            result = await self.command(args)
            if result is None:
                return Delivery(
                    "rejected", ValueError("RedisStreams NOMKSTREAM: stream absent")
                )
            return Delivery("published", id=result.decode())
        except ResponseError as e:
            return Delivery("rejected", e)
        except (Exception, asyncio.CancelledError):
            return Delivery(
                "unknown",
                TransportStopped(
                    "RedisStreams XADD reply unavailable; never automatically replayed"
                ),
            )

    async def ack(self, identity, epoch, ctx):
        if epoch != self.epoch or not self.ready:
            raise TransportStopped("RedisStreams ACK handle expired")
        return await self.command(
            ["XACK", self.ep.stream_key, self.ep.consumer_group, identity], ctx
        )

    async def read(self, count, block, ctx):
        response = await self.command(
            [
                "XREADGROUP",
                "GROUP",
                self.ep.consumer_group,
                self.consumer_name,
                "COUNT",
                count,
                "BLOCK",
                block,
                "STREAMS",
                self.ep.stream_key,
                ">",
            ],
            ctx,
            read=True,
            timeout=block / 1000 + self.timeout,
        )
        return [
            entry for _, batch in (response or []) for entry in entries(batch, False)
        ]

    async def claim(self, cursor, count, ctx):
        reply = await self.command(
            [
                "XAUTOCLAIM",
                self.ep.stream_key,
                self.ep.consumer_group,
                self.consumer_name,
                self.ep.claim_idle_ms or 60000,
                cursor,
                "COUNT",
                count,
            ],
            ctx,
        )
        return (
            reply[0].decode(),
            entries(reply[1], True),
            tuple(v.decode() for v in reply[2]),
        )

    def pause(self):
        self.paused = True
        # Read uses a separate pool; its task is cancelled by the source before drain.

    async def close(self, ctx):
        if self.stopped:
            return
        self.stopped = True
        self.ready = False
        self.epoch += 1
        self.unsubscribe()
        if self.owner:
            self.owner.cancel()
        if self.recovery and self.recovery is not asyncio.current_task():
            self.recovery.cancel()
        for task in tuple(self.operations):
            task.cancel()
        if self.reporter:
            self.reporter.cancel()
        await self.disconnect()

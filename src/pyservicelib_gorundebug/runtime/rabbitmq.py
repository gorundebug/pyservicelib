"""Owned official AMQP 1.0 SDK processes with bounded asyncio admission.

Stable SDK 1.x has blocking operations without public timeout controls. A process
boundary permits deadline termination without touching private Proton state.
"""

from __future__ import annotations
import asyncio
import multiprocessing as mp
import queue
import time
import ssl
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar
from urllib.parse import urlsplit, quote
from opentelemetry import context as otel_context, propagate
from rabbitmq_amqp_python_client import Message
from .context import Context
from .context.request import (
    request_cancelled,
    request_deadline,
    request_context_error,
    stream_id_from_context,
)
from .environment.tracing import (
    sampling_enabled,
    sampling_scope,
    sampling_requested_by_carrier,
)

BYTE_LIMIT = 8 * 1024 * 1024
MAX_MESSAGE_BYTES = 1024 * 1024
QUEUE_LIMIT = 4096
_incoming_trace_marker = ContextVar("rabbitmq_trace_marker", default=None)


class TransportStopped(RuntimeError):
    pass


class AdmissionRejected(RuntimeError):
    pass


@dataclass(frozen=True)
class Delivery:
    outcome: str
    error: Exception | None = None
    native_state: int | None = None
    reason: str | None = None


async def wait_context(
    operation, owner: Context | None = None, *, timeout=None, owned=True
):
    """Wait without blocking the loop; optionally leave an admitted operation owned."""
    task = asyncio.ensure_future(operation)
    if not owned:

        def observe(future):
            if not future.cancelled():
                future.exception()

        task.add_done_callback(observe)
    event = asyncio.Event() if owner is not None else request_cancelled.get()
    remove = (
        owner.add_cancel_callback(event.set)
        if owner is not None and event is not None
        else lambda: None
    )
    remaining = owner.time_left if owner is not None else None
    if owner is None and (deadline := request_deadline.get()) is not None:
        remaining = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
    if remaining is not None:
        timeout = remaining if timeout is None else min(timeout, remaining)
    cancel = asyncio.create_task(event.wait()) if event is not None else None
    try:
        if event is not None and event.is_set():
            raise TransportStopped("RabbitMQ request context cancelled")
        done, _ = await asyncio.wait(
            [task] + ([cancel] if cancel else []),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            return task.result()
        if cancel in done:
            raise TransportStopped("RabbitMQ request context cancelled")
        raise TimeoutError(
            "RabbitMQ operation deadline exceeded; publication may be unknown"
        )
    finally:
        remove()
        if cancel is not None:
            cancel.cancel()
            await asyncio.gather(cancel, return_exceptions=True)
        if owned and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


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


def validate_connection(cfg):
    if _value(cfg.type) != 9 or _value(cfg.implementation) not in (
        None,
        "",
        "rabbitmq-amqp-python-client",
    ):
        raise ValueError(
            "RabbitMQ requires the official AMQP 1.0 Python implementation"
        )
    if not cfg.servers:
        raise ValueError("RabbitMQ servers must be explicit")
    secure, plain = False, False
    for server in cfg.servers:
        try:
            url = urlsplit(server)
            if (
                url.scheme not in ("amqp", "amqps")
                or not url.hostname
                or url.username is not None
                or url.password is not None
                or url.path
                or "?" in server
                or "#" in server
                or any(c.isspace() or unicodedata.category(c) == "Cc" for c in server)
                or (url.port is not None and not 1 <= url.port <= 65535)
                or url.netloc.endswith(":")
            ):
                raise ValueError()
        except ValueError:
            raise ValueError(
                "RabbitMQ server URL must have a valid host/port and no credentials, path, query or fragment"
            ) from None
        secure |= url.scheme == "amqps"
        plain |= url.scheme == "amqp"
        if cfg.tls_server_name and cfg.tls_server_name != url.hostname:
            raise ValueError(
                "RabbitMQ Python SDK 1.x cannot override TLS server name; use a matching server hostname"
            )
    if secure and plain and not cfg.tls_enabled:
        raise ValueError("mixed RabbitMQ TLS URLs require tlsEnabled")
    if not (secure or cfg.tls_enabled) and any(
        (cfg.tls_server_name, cfg.tls_ca_file, cfg.tls_cert_file, cfg.tls_key_file)
    ):
        raise ValueError("TLS settings require TLS transport")
    if bool(cfg.username) != bool(cfg.password):
        raise ValueError("RabbitMQ username and password must both be configured")
    if cfg.token or cfg.credentials_file:
        raise ValueError(
            "RabbitMQ does not support token/credentialsFile authentication"
        )
    if bool(cfg.tls_cert_file) != bool(cfg.tls_key_file):
        raise ValueError("RabbitMQ TLS certificate and key must both be configured")
    for value in (
        2000 if cfg.connect_timeout_ms is None else cfg.connect_timeout_ms,
        5000 if cfg.operation_timeout_ms is None else cfg.operation_timeout_ms,
    ):
        if type(value) is not int or not 1 <= value <= 9223372036854:
            raise ValueError("RabbitMQ timeout must be positive milliseconds")
    vhost = cfg.virtual_host or "/"
    if quote(vhost.replace("+", "%2B")) != vhost:
        raise ValueError(
            "RabbitMQ Python SDK 1.x virtualHost must use URI-unreserved characters or slash"
        )


def validate_endpoint(ep, *, source):
    if (
        source
        and not ep.queue_name
        or not source
        and not (ep.queue_name or ep.exchange_name)
    ):
        raise ValueError(
            "RabbitMQ source requires queueName; sink requires queueName or exchangeName"
        )
    if (ep.create_queue or ep.queue_type) and not ep.queue_name:
        raise ValueError("queue settings require queueName")
    if (
        ep.create_exchange or ep.exchange_type or ep.routing_key
    ) and not ep.exchange_name:
        raise ValueError("exchange settings require exchangeName")
    if ep.create_binding and not (ep.queue_name and ep.exchange_name):
        raise ValueError("createBinding requires queueName and exchangeName")
    if _value(ep.queue_type) not in (None, "", "quorum", "classic"):
        raise ValueError("RabbitMQ supports classic/quorum queues")
    if _value(ep.exchange_type) not in (None, "", "direct", "topic", "fanout"):
        raise ValueError("RabbitMQ exchange type must be direct/topic/fanout")
    if ep.exchange_type and not ep.create_exchange:
        raise ValueError(
            "exchangeType requires createExchange; native read-only inspection is unavailable"
        )
    fields = getattr(ep, "model_fields_set", set())
    if (
        source
        and "publisher_max_in_flight" in fields
        or not source
        and "consumer_credits" in fields
    ):
        raise ValueError(
            "RabbitMQ credit/pending configuration has wrong endpoint direction"
        )
    for v in (
        256 if ep.consumer_credits is None else ep.consumer_credits,
        256 if ep.publisher_max_in_flight is None else ep.publisher_max_in_flight,
    ):
        if type(v) is not int or not 1 <= v <= QUEUE_LIMIT:
            raise ValueError("RabbitMQ credit/pending limit must be 1..4096")
    for name in (ep.queue_name, ep.exchange_name, ep.routing_key):
        if any(unicodedata.category(c) == "Cc" for c in name or ""):
            raise ValueError("RabbitMQ names cannot contain controls")


def _wire_config(cfg, ep):
    # Only primitive values cross the owner boundary. Never log the internal URI.
    return (
        {
            k: _value(getattr(cfg, k))
            for k in (
                "servers",
                "username",
                "password",
                "virtual_host",
                "tls_enabled",
                "tls_ca_file",
                "tls_cert_file",
                "tls_key_file",
            )
        },
        {
            k: _value(getattr(ep, k))
            for k in (
                "queue_name",
                "exchange_name",
                "routing_key",
                "queue_type",
                "exchange_type",
                "create_queue",
                "create_exchange",
                "create_binding",
                "consumer_credits",
            )
        },
    )


def _connect_native(cfg, index):
    import rabbitmq_amqp_python_client as sdk

    server = urlsplit(cfg["servers"][index % len(cfg["servers"])])
    secure = cfg["tls_enabled"] or server.scheme == "amqps"
    host = f"[{server.hostname}]" if ":" in server.hostname else server.hostname
    authority = host + (f":{server.port}" if server.port else "")
    if cfg["username"]:
        authority = (
            quote(cfg["username"], safe="")
            + ":"
            + quote(cfg["password"], safe="")
            + "@"
            + authority
        )
    vhost = cfg["virtual_host"] or "/"
    uri = (
        ("amqps" if secure else "amqp")
        + "://"
        + authority
        + ("/" if vhost == "/" else "/" + vhost)
    )
    tls = None
    if secure:
        ca = cfg["tls_ca_file"] or ssl.get_default_verify_paths().cafile
        if not ca:
            raise ValueError("TLS requires a usable CA bundle")
        cert = (
            sdk.PosixClientCert(cfg["tls_cert_file"], cfg["tls_key_file"])
            if cfg["tls_cert_file"]
            else None
        )
        tls = sdk.PosixSslConfigurationContext(ca, cert)
    conn = sdk.Connection(
        uri=uri,
        ssl_context=tls,
        recovery_configuration=sdk.RecoveryConfiguration(active_recovery=False),
    )
    conn.dial()
    return conn


def _topology(conn, ep):
    import rabbitmq_amqp_python_client as sdk

    manager = conn.management()
    if ep["queue_name"]:
        # Public native management request allows expected 404 without parsing
        # SDK exception text or treating permission errors as absence.
        response = manager.request(
            None, sdk.AddressHelper.queue_address(ep["queue_name"]), "GET", [200, 404]
        )
        if int(response.subject) == 404:
            if not ep["create_queue"]:
                raise ValueError("RabbitMQ queue does not exist; creation is disabled")
            spec = (
                sdk.ClassicQueueSpecification
                if ep["queue_type"] == "classic"
                else sdk.QuorumQueueSpecification
            )
            manager.declare_queue(spec(ep["queue_name"]))
        info = manager.queue_info(ep["queue_name"])
        if (
            info.queue_type.value not in ("classic", "quorum")
            or ep["queue_type"]
            and info.queue_type.value != ep["queue_type"]
        ):
            raise ValueError("RabbitMQ existing queue type is incompatible")
    if ep["create_exchange"]:
        manager.declare_exchange(
            sdk.ExchangeSpecification(
                ep["exchange_name"],
                exchange_type=sdk.ExchangeType(ep["exchange_type"] or "direct"),
            )
        )
    if ep["create_binding"]:
        manager.bind(
            sdk.ExchangeToQueueBindingSpecification(
                ep["exchange_name"], ep["queue_name"], ep["routing_key"] or ""
            )
        )


def _native_owner(cfg, ep, source, commands, results, index):
    """Spawn entrypoint: SDK connection, links and delivery handles stay here."""
    import rabbitmq_amqp_python_client as sdk
    from rabbitmq_amqp_python_client.delivery_context import DeliveryContext
    from rabbitmq_amqp_python_client.qpid.proton.utils import SendException
    import logging

    logging.disable(
        logging.CRITICAL
    )  # SDK diagnostics may include credential-bearing internal URIs.
    conn = None
    try:
        conn = _connect_native(cfg, index)
        results.put_nowait(("connected", 0, None))
        _topology(conn, ep)
        if source:

            class Receiver(sdk.MessagingHandler):
                def __init__(self):
                    super().__init__(prefetch=0, auto_accept=False, auto_settle=False)
                    self.deliveries, self.serial, self.timer = {}, 0, False
                    self.bytes = 0
                    self.accepting = True
                    self.settlement = DeliveryContext()

                def on_link_opened(self, event):
                    if event.receiver and not self.timer:
                        self.timer = True
                        event.container.schedule(0.01, self)

                def on_message(self, event):
                    if not self.accepting:
                        return  # Leave prefetched deliveries unsettled for close/redelivery.
                    encoded = event.message.encode()
                    if (
                        len(encoded) > MAX_MESSAGE_BYTES
                        or self.bytes + len(encoded) > BYTE_LIMIT
                        or len(self.deliveries) >= (ep["consumer_credits"] or 256)
                    ):
                        raise RuntimeError("RabbitMQ receive capacity exceeded")
                    self.serial += 1
                    from types import SimpleNamespace

                    self.deliveries[self.serial] = SimpleNamespace(
                        delivery=event.delivery,
                        receiver=event.receiver,
                        size=len(encoded),
                    )
                    self.bytes += len(encoded)
                    results.put_nowait(("message", self.serial, encoded))

                def on_timer_task(self, event):
                    for _ in range(64):
                        try:
                            command = commands.get_nowait()
                        except queue.Empty:
                            break
                        kind, serial, value = command
                        if kind == "close":
                            event.container.stop()
                            return
                        if kind == "pause":
                            self.accepting = False
                        elif kind == "settle":
                            delivery = self.deliveries.pop(value[0], None)
                            if delivery is None:
                                results.put_nowait(
                                    ("settled", serial, "stale delivery")
                                )
                                continue
                            getattr(self.settlement, value[1])(delivery)
                            self.bytes -= delivery.size
                            if self.accepting:
                                delivery.receiver.flow(1)
                            results.put_nowait(("settled", serial, None))
                    event.container.schedule(0.01, self)

            if ep["exchange_name"] and not ep["create_exchange"]:
                probe = conn.publisher(
                    sdk.AddressHelper.exchange_address(
                        ep["exchange_name"], ep["routing_key"] or ""
                    )
                )
                probe.close()
            receiver = conn.consumer(
                sdk.AddressHelper.queue_address(ep["queue_name"]),
                Receiver(),
                sdk.ConsumerOptions(
                    settle_strategy=sdk.ConsumerSettleStrategy.ExplicitSettle
                ),
                credit=ep["consumer_credits"] or 256,
            )
            results.put_nowait(("ready", 0, None))
            receiver.run()
        else:
            address = (
                sdk.AddressHelper.exchange_address(
                    ep["exchange_name"], ep["routing_key"] or ""
                )
                if ep["exchange_name"]
                else sdk.AddressHelper.queue_address(ep["queue_name"])
            )
            publisher = conn.publisher(address)
            results.put_nowait(("ready", 0, None))
            while True:
                kind, serial, value = commands.get()
                if kind == "close":
                    break
                if kind != "publish":
                    continue
                encoded, expires = value
                if time.monotonic() >= expires:
                    results.put_nowait(
                        (
                            "delivery",
                            serial,
                            (
                                "not-sent",
                                None,
                                "publication expired before transmission",
                            ),
                        )
                    )
                    continue
                results.put_nowait(("sending", serial, None))
                message = sdk.Message()
                message.decode(encoded)
                if isinstance(message.body, memoryview):
                    message.body = bytes(message.body)
                try:
                    native = publisher.publish(message)
                    state = native.remote_state
                    outcome = {
                        sdk.Delivery.ACCEPTED: "accepted",
                        sdk.Delivery.RELEASED: "unroutable",
                        sdk.Delivery.REJECTED: "rejected",
                        sdk.Delivery.MODIFIED: "modified",
                    }.get(state, "unknown")
                    results.put_nowait(
                        (
                            "delivery",
                            serial,
                            (outcome, int(state) if state is not None else None, None),
                        )
                    )
                except sdk.AmqpMessageRejectedException as error:
                    results.put_nowait(
                        (
                            "delivery",
                            serial,
                            ("rejected", int(sdk.Delivery.REJECTED), str(error)),
                        )
                    )
                except SendException as error:
                    results.put_nowait(
                        (
                            "delivery",
                            serial,
                            ("unroutable", int(sdk.Delivery.RELEASED), None),
                        )
                    )
    except BaseException as error:
        # Most SDK validation exceptions inherit BaseException. Never forward its
        # text because Proton errors can contain the credential-bearing URI.
        results.put(
            (
                "error",
                0,
                f"RabbitMQ native {type(error).__name__}; connection/permissions/topology require checking",
            )
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except BaseException:
                pass
        try:
            results.put_nowait(("closed", 0, None))
        except queue.Full:
            pass


class Session:
    """One bounded, reconnectable SDK owner per endpoint; never replay sends."""

    def __init__(self, cfg, ep, *, source, report):
        validate_connection(cfg)
        validate_endpoint(ep, source=source)
        self.cfg, self.ep, self.source, self.report = cfg, ep, source, report
        self.timeout = (cfg.operation_timeout_ms or 5000) / 1000
        self.connect_timeout = (cfg.connect_timeout_ms or 2000) / 1000
        self.limit = (
            ep.consumer_credits if source else ep.publisher_max_in_flight
        ) or 256
        self.messages = asyncio.Queue(maxsize=self.limit)
        self.pending = {}
        self.serial = 0
        self.bytes = 0
        self.epoch = 0
        self.closed = False
        self.accepting = False
        self.ready = asyncio.Event()
        self.process = None
        self.task = None
        self.owner = None
        self.failure = None
        self._closing = False
        self._paused = False

    async def start(self, ctx):
        if self.task is not None or self.closed:
            raise TransportStopped("RabbitMQ lifecycle already started/stopped")
        self.owner = ctx.child()
        self.started = asyncio.get_running_loop().create_future()
        self.task = asyncio.create_task(self._supervise())
        try:
            await wait_context(
                self.started,
                ctx,
                timeout=self.connect_timeout + self.timeout,
                owned=False,
            )
        except BaseException:
            await self.close(Context())
            raise
        self.accepting = True

    def _spawn(self):
        context = mp.get_context("spawn")
        self.commands = context.Queue(maxsize=self.limit + 4)
        self.results = context.Queue(maxsize=2 * self.limit + 8)
        cfg, ep = _wire_config(self.cfg, self.ep)
        self.process = context.Process(
            target=_native_owner,
            args=(cfg, ep, self.source, self.commands, self.results, self.epoch),
            daemon=True,
        )
        self.process.start()
        self.epoch += 1
        self.ready.clear()
        while not self.messages.empty():
            self.messages.get_nowait()

    async def _dispose(self):
        process, self.process = self.process, None
        if process is not None:
            if process.is_alive():
                process.kill()
            # Reaping does not touch SDK state; no blocking native operation on asyncio.
            await asyncio.to_thread(process.join, 1)
            process.close()
        for q in (getattr(self, "commands", None), getattr(self, "results", None)):
            if q is not None:
                q.cancel_join_thread()
                q.close()

    def _fail_pending(self, error):
        for future, _, _, kind in self.pending.values():
            if not future.done():
                if kind == "publish":
                    future.set_result(Delivery("unknown", error))
                else:
                    future.set_exception(error)
        self.pending.clear()
        self.bytes = 0

    async def _supervise(self):
        attempts = 0
        try:
            while not self.closed and not self.owner.is_expired:
                self.failure = None
                self._spawn()
                deadline = time.monotonic() + self.connect_timeout
                while not self.closed and not self.owner.is_expired:
                    try:
                        kind, serial, value = self.results.get_nowait()
                    except queue.Empty:
                        if not self.process.is_alive():
                            self.failure = TransportStopped(
                                "RabbitMQ native owner exited"
                            )
                            break
                        if not self.ready.is_set() and time.monotonic() >= deadline:
                            self.failure = TimeoutError(
                                "RabbitMQ connection/topology startup deadline exceeded"
                            )
                            break
                        if any(
                            time.monotonic() >= item[1]
                            for item in self.pending.values()
                        ):
                            self.failure = TimeoutError(
                                "RabbitMQ native operation deadline exceeded; publication outcome unknown"
                            )
                            break
                        await asyncio.sleep(0.005)
                        continue
                    if kind == "connected":
                        deadline = time.monotonic() + self.timeout
                    elif kind == "ready":
                        self.ready.set()
                        if not self.started.done():
                            self.started.set_result(None)
                    elif kind == "error":
                        self.failure = TransportStopped(value)
                        break
                    elif kind == "closed":
                        break
                    elif kind == "message":
                        if not self._paused:
                            self.messages.put_nowait((value, serial, self.epoch))
                    elif kind in ("delivery", "settled"):
                        item = self.pending.pop(serial, None)
                        if item:
                            future, _, size, _ = item
                            self.bytes -= size
                            if not future.done():
                                if kind == "delivery":
                                    outcome, native_state, reason = value
                                    error = (
                                        AdmissionRejected(reason)
                                        if outcome == "not-sent"
                                        else None
                                    )
                                    future.set_result(
                                        Delivery(outcome, error, native_state, reason)
                                    )
                                elif value:
                                    future.set_exception(TransportStopped(value))
                                else:
                                    future.set_result(None)
                self.ready.clear()
                self.epoch += 1
                error = self.failure or TransportStopped("RabbitMQ connection closed")
                self._fail_pending(error)
                await self._dispose()
                if self.closed or self.owner.is_expired or self._closing:
                    break
                if not self.started.done():
                    self.started.set_exception(error)
                    break
                await wait_context(self.report(error), timeout=self.timeout)
                attempts += 1
                if attempts >= 60:
                    break
                await wait_context(asyncio.sleep(0.25), self.owner)
        except (Exception, asyncio.CancelledError) as error:
            self._fail_pending(TransportStopped("RabbitMQ owner stopped"))
            if not self.started.done():
                self.started.set_exception(TransportStopped("RabbitMQ startup failed"))
        finally:
            self.ready.clear()
            self.closed = True
            self._fail_pending(TransportStopped("RabbitMQ owner stopped"))
            await self._dispose()

    def _submit(self, kind, value, size=0, *, deadline=None):
        if self.closed or not self.ready.is_set() or self.owner.is_expired:
            raise AdmissionRejected("RabbitMQ is stopped or reconnecting")
        if len(self.pending) >= self.limit or self.bytes + size > BYTE_LIMIT:
            raise AdmissionRejected("RabbitMQ pending count/byte capacity exhausted")
        self.serial += 1
        future = asyncio.get_running_loop().create_future()
        expires = min(deadline or float("inf"), time.monotonic() + self.timeout)
        self.commands.put_nowait(
            (kind, self.serial, (value, expires) if kind == "publish" else value)
        )
        self.pending[self.serial] = (future, expires, size, kind)
        self.bytes += size
        return future

    def publish(self, message):
        if not self.accepting or request_context_error():
            raise AdmissionRejected("RabbitMQ request cancelled or admission stopped")
        if message.address is not None:
            raise ValueError(
                "RabbitMQ message address cannot override modeled destination"
            )
        if not isinstance(message.body, (bytes, type(None))) or not message.inferred:
            raise ValueError(
                "RabbitMQ SDK requires native Message with bytes body and inferred=True"
            )
        snapshot = Message()
        snapshot.decode(message.encode())
        snapshot.application_properties = outgoing_headers(
            message.application_properties or {}
        )
        encoded = snapshot.encode()
        if len(encoded) > MAX_MESSAGE_BYTES:
            raise AdmissionRejected("RabbitMQ message exceeds 1 MiB profile limit")
        deadline = request_deadline.get()
        expires = (
            None
            if deadline is None
            else time.monotonic()
            + max(0, (deadline - datetime.now(timezone.utc)).total_seconds())
        )
        return self._submit("publish", encoded, len(encoded), deadline=expires)

    async def settle(self, serial, epoch, action):
        if epoch != self.epoch or self.closed or not self.ready.is_set():
            raise TransportStopped(
                "RabbitMQ delivery belongs to an expired receiver generation"
            )
        future = self._submit("settle", (serial, action))
        await wait_context(future, timeout=self.timeout, owned=False)

    def pause(self):
        self.accepting = False
        self._paused = True
        if self.ready.is_set() and not self.closed:
            self.commands.put_nowait(("pause", 0, None))

    async def close(self, ctx):
        self.accepting = False
        self._closing = True
        if self.process and self.process.is_alive():
            try:
                self.commands.put_nowait(("close", 0, None))
            except queue.Full:
                pass
        try:
            if self.task:
                await wait_context(
                    asyncio.shield(self.task), ctx, timeout=self.timeout, owned=False
                )
        except (Exception, asyncio.CancelledError):
            self.closed = True
            self.epoch += 1
            self.ready.clear()
            self._fail_pending(TransportStopped("RabbitMQ shutdown deadline reached"))
            if self.process and self.process.is_alive():
                self.process.kill()
            if self.task:
                self.task.cancel()
                # Reaping is finite cleanup owned by the supervisor, not a new SDK operation.

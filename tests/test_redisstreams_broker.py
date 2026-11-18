"""Native Redis acceptance with unique keys; never mutate shared broker configuration."""

import asyncio
import os
import ssl
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlsplit

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from pyservicelib_gorundebug.datasink.redisstreams import (
    make_redisstreams_endpoint_consumer_with_result as sink_factory,
)
from pyservicelib_gorundebug.datasource.redisstreams import (
    make_redisstreams_endpoint_consumer as source_factory,
)
from pyservicelib_gorundebug.runtime.config import (
    RedisStreamsDataConnectorConfig,
    RedisStreamsEndpointConfig,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import (
    request_context_scope,
    stream_id_from_context,
    with_stream_id,
)
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as RecordingMetrics
from pyservicelib_gorundebug.runtime.redisstreams import (
    AdmissionRejected,
    DeletedPendingEntriesError,
    Session,
    TransportStopped,
    snapshot,
)


@pytest.fixture
def broker():
    value = os.environ.get("REDIS_TEST_URL")
    if not value:
        pytest.skip("set REDIS_TEST_URL to shared Redis 7.2+ primary")
    return value


@pytest_asyncio.fixture(loop_scope="function")
async def resources(broker):
    clients = []
    keys = []

    async def client(db=0):
        c = Redis.from_url(
            broker, password="adapter-test", db=db, decode_responses=False
        )
        clients.append(c)
        return c

    def key():
        value = "py-redis-" + uuid.uuid4().hex
        keys.append(value)
        return value

    yield SimpleNamespace(client=client, key=key, keys=keys)
    for db in (0, 1):
        c = await client(db)
        if keys:
            await c.delete(*keys)
    for c in clients:
        await c.aclose()


async def until(predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def report(error):
    pass


def setup(url, *, result=False, **endpoint_options):
    cfg = RedisStreamsDataConnectorConfig(
        id=1,
        name="broker",
        servers=[url],
        password="adapter-test",
        connectTimeoutMs=4000,
        operationTimeoutMs=1500,
    )
    values = {
        "id": 2,
        "name": "events",
        "idDataConnector": 1,
        "streamKey": "py-redis-" + uuid.uuid4().hex,
        "enabled": True,
        "functionName": "Handle",
    }
    values.update(endpoint_options)
    ep = RedisStreamsEndpointConfig(**values)
    sources, sinks = {}, {}
    env = SimpleNamespace(
        config=SimpleNamespace(
            get_data_connector_by_id=lambda _: cfg,
            get_endpoint_config_by_id=lambda _: ep,
        ),
        metrics=NoopMetrics(),
        log=Mock(),
        tracing=None,
    )
    env.get_datasource, env.get_datasink = sources.get, sinks.get
    env.add_datasource = lambda ds: sources.__setitem__(ds.id, ds)
    env.add_datasink = lambda ds: sinks.__setitem__(ds.id, ds)
    received, results, errors = [], [], []

    async def collect(value):
        received.append((value, stream_id_from_context()))

    async def out(value):
        results.append((value, stream_id_from_context()))

    async def error(value):
        errors.append((value, stream_id_from_context()))

    stream = SimpleNamespace(
        environment=env,
        endpoint_id=2,
        name="flow",
        consume=collect,
        consume_result=out,
        error_stream=SimpleNamespace(consume=error),
        get_result_stream=lambda: object() if result else None,
        set_sink_consumer=lambda c: setattr(stream, "sink_consumer", c),
        set_result_consumer=lambda c: setattr(stream, "result_consumer", c),
    )
    return SimpleNamespace(
        env=env,
        stream=stream,
        cfg=cfg,
        ep=ep,
        sources=sources,
        sinks=sinks,
        received=received,
        results=results,
        errors=errors,
    )


class SourceHandler:
    def __init__(self, mode="ack", concurrency=1):
        self.mode, self.count = mode, concurrency
        self.messages, self.errors, self.ended = [], [], []
        self.gate = asyncio.Event()

    def concurrency(self, sc):
        return self.count

    async def on_error(self, sc, error):
        self.errors.append(error)

    async def begin_request(self, sc):
        return "state"

    async def consume_message(self, sc, state, msg, result):
        self.messages.append(msg)
        if self.mode == "hold":
            await self.gate.wait()
        if self.mode == "callback":

            async def callback(sc, state, value):
                await msg.ack()
                result.done()
                return True

            result.set_result_callback("event", callback)
            await sc.collect(msg.data)
        else:
            if self.mode in ("ack", "hold"):
                assert await msg.ack() == 1
            result.done()

    def get_message_id(self, sc, state, value):
        return "event"

    async def end_request(self, sc, error, state):
        self.ended.append(error)


class SinkHandler:
    def __init__(self, sync=False):
        self.sync, self.ended = sync, []

    def get_stream_id(self, value):
        return "publication"

    def begin_request(self, stream):
        return "state"

    async def consume_message(self, stream, state, value, msg):
        msg.fields = value
        if self.sync:
            await msg.out(await msg.send_sync())
        else:
            msg.send(lambda d: d)

    async def end_request(self, stream, error, state):
        self.ended.append(error)


async def stop(f):
    ctx = Context(timedelta(seconds=1))
    for s in list(f.sources.values()) + list(f.sinks.values()):
        await s.stop(ctx)
    await asyncio.sleep(0.02)


def source(broker, key, handler=None, **kw):
    f = setup(
        broker,
        streamKey=key,
        consumerGroup="workers",
        createConsumerGroup=True,
        readBlockMs=50,
        claimIntervalMs=30,
        claimIdleMs=60000,
        **kw,
    )
    h = handler or SourceHandler()
    consumer = source_factory(f.stream, h)
    return f, h, consumer


def sink(broker, key, handler=None, **kw):
    f = setup(broker, streamKey=key, **kw)
    h = handler or SinkHandler()
    consumer = sink_factory(f.stream, h)
    return f, h, consumer


@asynccontextmanager
async def native(broker, key, source=False, **kw):
    f = setup(broker, streamKey=key, **kw)
    s = Session(f.cfg, f.ep, source=source, report=report)
    await s.start(Context())
    try:
        yield s
    finally:
        await s.close(Context())


@pytest.mark.asyncio
async def test_graph_native_binary_duplicates_done_not_ack(broker, resources):
    key = resources.key()
    c = await resources.client()
    f, h, _ = source(broker, key, SourceHandler("done"))
    p, _sh, publisher = sink(broker, key)
    try:
        await f.sources[1].start(Context())
        await p.sinks[1].start(Context())
        fields = [
            (b"duplicate", b"one"),
            (b"duplicate", b"two"),
            (b"binary", bytes([0, 255])),
            (b"caller", b"retained"),
        ]
        with request_context_scope(Context()):
            with_stream_id("inherited")
            await publisher.consume(fields)
        await until(lambda: h.ended and p.results)
        delivery, sid = p.results[0]
        assert delivery.outcome == "published" and sid == "inherited"
        assert h.messages[0].message.id == delivery.id
        assert h.messages[0].data[:4] == tuple(fields)
        assert h.messages[0].headers["x-stream-id"] == "inherited"
        assert (await c.xpending(key, "workers"))["pending"] == 1
        with pytest.raises(TransportStopped):
            await h.messages[0].ack()
        assert await c.xlen(key) == 1
    finally:
        await stop(f)
        await stop(p)


@pytest.mark.asyncio
async def test_synchronous_result_callback_registration_and_ack(broker, resources):
    key = resources.key()
    c = await resources.client()
    f, h, consumer = source(broker, key, SourceHandler("callback"), result=True)

    async def collect(value):
        await f.stream.result_consumer.consume(value)

    consumer.sc.collect = collect
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"event": "one"})
        await until(lambda: h.ended)
        assert h.ended == [None] and (await c.xpending(key, "workers"))["pending"] == 0
        assert await c.xlen(key) == 1
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_creation_nomkstream_wrongtype_and_group_position(broker, resources):
    key = resources.key()
    c = await resources.client()
    async with native(broker, key) as s:
        assert (await s.publish(((b"a", b"b"),))).outcome == "rejected"
        assert not await c.exists(key)
    async with native(broker, key, createStream=True) as s:
        assert (await s.publish(((b"a", b"b"),))).outcome == "published"
        await c.delete(key)
        await c.set(key, "wrong")
        assert (await s.publish(((b"a", b"b"),))).outcome == "rejected"
    await c.delete(key)
    f = setup(broker, streamKey=key, consumerGroup="g")
    s = Session(f.cfg, f.ep, source=True, report=report)
    with pytest.raises(TransportStopped):
        await s.start(Context())
    await c.xadd(key, {"old": "entry"})
    async with native(
        broker,
        key,
        source=True,
        consumerGroup="g",
        createConsumerGroup=True,
        groupStartId="$",
    ) as s:
        assert await s.read(1, 10, Context()) == []
    identity = await c.xadd(key, {"new": "entry"})
    async with native(
        broker,
        key,
        source=True,
        consumerGroup="g",
        createConsumerGroup=True,
        groupStartId="0-0",
    ) as s:
        batch = await s.read(1, 10, Context())
        assert batch[0].id == identity.decode()
        assert await s.ack(batch[0].id, s.epoch, Context()) == 1


@pytest.mark.asyncio
async def test_reclaim_empty_cursor_pages_and_deleted_ids(broker, resources):
    key = resources.key()
    c = await resources.client()
    await c.xgroup_create(key, "workers", id="0-0", mkstream=True)
    ids = [await c.xadd(key, {"n": str(i)}) for i in range(12)]
    await c.xreadgroup("workers", "old", {key: ">"}, count=12)
    await c.xclaim(key, "workers", "old", 0, ids[-2:], idle=200000)
    await c.xdel(key, ids[-1])
    f = setup(
        broker,
        streamKey=key,
        consumerGroup="workers",
        readCount=1,
        readBlockMs=5,
        claimIntervalMs=10,
        claimIdleMs=100000,
    )
    f.env.metrics = RecordingMetrics()
    h = SourceHandler()
    source_factory(f.stream, h)
    try:
        await f.sources[1].start(Context())
        await until(
            lambda: (
                len(h.messages) == 1
                and any(isinstance(e, DeletedPendingEntriesError) for e in h.errors)
            )
        )
        assert (
            h.messages[0].message.id == ids[-2].decode()
            and h.messages[0].message.reclaimed
        )
        assert next(
            e for e in h.errors if isinstance(e, DeletedPendingEntriesError)
        ).ids == (ids[-1].decode(),)
        labels = {"protocol": "redisstreams", "connector": f.cfg.name, "endpoint": f.ep.name}
        for observation in ("received", "reclaimed"):
            assert f.env.metrics.counter("broker_endpoint_source_observations_total", {**labels, "observation": observation}).count() == 1
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_bounded_source_and_ack_during_drain(broker, resources):
    key = resources.key()
    c = await resources.client()
    f, h, _ = source(broker, key, SourceHandler("hold", 2), readCount=2)
    try:
        await f.sources[1].start(Context())
        for i in range(6):
            await c.xadd(key, {"n": str(i)})
        await until(lambda: len(h.messages) == 2)
        await asyncio.sleep(0.08)
        assert len(h.messages) == 2
        draining = asyncio.create_task(f.sources[1].stop(Context(timedelta(seconds=1))))
        await asyncio.sleep(0.03)
        assert not draining.done()
        h.gate.set()
        await draining
        assert h.ended == [None, None]
        assert (await c.xpending(key, "workers"))["pending"] == 0
        assert await c.xlen(key) == 6
    finally:
        h.gate.set()
        await stop(f)


@pytest.mark.asyncio
async def test_sink_callbacks_hold_capacity_after_end_and_stop_is_finite(
    broker, resources
):
    gate = asyncio.Event()
    entered = asyncio.Event()

    class H(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            msg.fields = value

            async def callback(d):
                entered.set()
                try:
                    await gate.wait()
                except asyncio.CancelledError:
                    await gate.wait()
                return d

            msg.send(callback)

    f, h, p = sink(
        broker, resources.key(), H(), createStream=True, publisherMaxInFlight=1
    )
    try:
        await f.sinks[1].start(Context())
        await p.consume([("a", "b")])
        await entered.wait()
        assert h.ended == [None]
        await p.consume([("a", "c")])
        assert isinstance(h.ended[-1], AdmissionRejected)
        start = time.monotonic()
        await f.sinks[1].stop(Context(timedelta(milliseconds=60)))
        assert time.monotonic() - start < 0.4
    finally:
        gate.set()
        await stop(f)


@pytest.mark.asyncio
async def test_noncooperative_source_end_does_not_race_stop(broker, resources):
    gate = asyncio.Event()
    entered = asyncio.Event()

    class H(SourceHandler):
        async def consume_message(self, sc, state, msg, result):
            self.messages.append(msg)
            entered.set()
            try:
                await gate.wait()
            except asyncio.CancelledError:
                await gate.wait()
            result.done()

    f, h, _ = source(broker, resources.key(), H())
    c = await resources.client()
    try:
        await f.sources[1].start(Context())
        await c.xadd(f.ep.stream_key, {"a": "b"})
        await entered.wait()
        start = time.monotonic()
        await f.sources[1].stop(Context(timedelta(milliseconds=50)))
        assert time.monotonic() - start < 0.4 and not h.ended
        assert (await c.xpending(f.ep.stream_key, "workers"))["pending"] == 1
        with pytest.raises(TransportStopped):
            await h.messages[0].ack()
    finally:
        gate.set()
        await until(lambda: h.ended)
        await stop(f)


class Proxy:
    def __init__(self, url, tls=None):
        self.target = urlsplit(url)
        self.tls = tls
        self.drop = False
        self.writers = set()
        self.tasks = set()

    async def __aenter__(self):
        async def accept(reader, writer):
            self.writers.add(writer)
            upstream, out = await asyncio.open_connection(
                self.target.hostname, self.target.port or 6379
            )
            self.writers.add(out)

            async def pump(r, w, reply=False):
                try:
                    while data := await r.read(65536):
                        if not reply or not self.drop:
                            w.write(data)
                            await w.drain()
                finally:
                    w.close()

            tasks = [
                asyncio.create_task(pump(reader, out)),
                asyncio.create_task(pump(upstream, writer, True)),
            ]
            self.tasks.update(tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.difference_update(tasks)

        self.server = await asyncio.start_server(accept, "127.0.0.1", 0, ssl=self.tls)
        self.url = (
            "rediss" if self.tls else "redis"
        ) + f"://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    def cut(self):
        for w in self.writers:
            w.close()

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        self.cut()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_lost_xadd_reply_unknown_no_replay_and_recovery(broker, resources):
    key = resources.key()
    c = await resources.client()
    async with Proxy(broker) as proxy:
        f = setup(proxy.url, streamKey=key, createStream=True)
        f.cfg.operation_timeout_ms = 100
        s = Session(f.cfg, f.ep, source=False, report=report)
        await s.start(Context())
        try:
            proxy.drop = True
            d = await s.publish(((b"a", b"b"),))
            assert d.outcome == "unknown" and await c.xlen(key) == 1
            proxy.drop = False
            await until(lambda: s.ready)
            await asyncio.sleep(0.1)
            assert await c.xlen(key) == 1
            assert (await s.publish(((b"a", b"c"),))).outcome == "published"
            assert await c.xlen(key) == 2
        finally:
            await s.close(Context())


@pytest.mark.asyncio
async def test_source_interruption_reclaim_and_old_handle_fence(broker, resources):
    key = resources.key()
    c = await resources.client()
    async with Proxy(broker) as proxy:
        f = setup(
            proxy.url,
            streamKey=key,
            consumerGroup="workers",
            createConsumerGroup=True,
            readBlockMs=20,
            claimIdleMs=20,
            claimIntervalMs=20,
        )
        h = SourceHandler("hold", 2)
        consumer = source_factory(f.stream, h)
        try:
            await f.sources[1].start(Context())
            await c.xadd(key, {"a": "b"})
            await until(lambda: h.messages)
            old = h.messages[0]
            proxy.cut()
            await until(lambda: consumer.connection.epoch != old.epoch)
            with pytest.raises(TransportStopped):
                await old.ack()
            h.gate.set()
            await until(lambda: len(h.messages) >= 2)
            await until(lambda: any(e is None for e in h.ended))
            assert (await c.xpending(key, "workers"))["pending"] == 0
        finally:
            h.gate.set()
            await stop(f)


@pytest.mark.asyncio
async def test_multiple_consumers_unique_names_and_native_database(broker, resources):
    key = resources.key()
    c = await resources.client()
    a, ha, ca = source(broker, key)
    b, hb, cb = source(broker, key)
    try:
        await a.sources[1].start(Context())
        await b.sources[1].start(Context())
        assert ca.connection.consumer_name != cb.connection.consumer_name
        for i in range(12):
            await c.xadd(key, {"n": str(i)})
        await until(lambda: len(ha.ended) + len(hb.ended) == 12)
        assert ha.messages and hb.messages
        assert (await c.xpending(key, "workers"))["pending"] == 0
    finally:
        await stop(a)
        await stop(b)
    f = setup(broker, streamKey=resources.key(), createStream=True)
    f.cfg.redis_database = 1
    s = Session(f.cfg, f.ep, source=False, report=report)
    try:
        await s.start(Context())
        assert (await s.publish(((b"db", b"1"),))).outcome == "published"
        c1 = await resources.client(1)
        assert await c1.xlen(f.ep.stream_key) == 1 and not await c.exists(
            f.ep.stream_key
        )
    finally:
        await s.close(Context())


@pytest.mark.asyncio
async def test_auth_startup_timeout_and_pre_admission_cancel(broker, resources):
    f = setup(broker, streamKey=resources.key(), createStream=True)
    f.cfg.password = "wrong"
    s = Session(f.cfg, f.ep, source=False, report=report)
    with pytest.raises(TransportStopped, match="startup failed"):
        await s.start(Context())
    async with Proxy(broker) as proxy:
        proxy.drop = True
        f = setup(proxy.url, streamKey=resources.key())
        f.cfg.connect_timeout_ms = 75
        s = Session(f.cfg, f.ep, source=False, report=report)
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            await s.start(Context())
        assert time.monotonic() - start < 0.5
    async with native(broker, resources.key(), createStream=True) as s:
        ctx = Context()
        ctx.cancel()
        with request_context_scope(ctx):
            assert (await s.publish(((b"a", b"b"),))).outcome == "not-sent"


@pytest.mark.asyncio
async def test_tls_ca_and_hostname_verification(broker, resources, tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    await asyncio.to_thread(
        subprocess.run,
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=redis-test",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    async with Proxy(broker, tls) as proxy:
        f = setup(proxy.url, streamKey=resources.key(), createStream=True)
        f.cfg.tls_ca_file = str(cert)
        s = Session(f.cfg, f.ep, source=False, report=report)
        try:
            await s.start(Context())
            assert (await s.publish(((b"a", b"b"),))).outcome == "published"
        finally:
            await s.close(Context())
        f.cfg.tls_ca_file = None
        bad = Session(f.cfg, f.ep, source=False, report=report)
        with pytest.raises(TransportStopped):
            await bad.start(Context())


@pytest.mark.asyncio
async def test_oversized_entries_left_pending_and_send_rejected(broker, resources):
    key = resources.key()
    c = await resources.client()
    f, h, _ = source(broker, key)
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"body": b"x" * (1024 * 1024)})
        await until(lambda: h.errors)
        assert not h.messages and (await c.xpending(key, "workers"))["pending"] == 1
        with pytest.raises(AdmissionRejected):
            snapshot([("body", b"x" * (1024 * 1024))])
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_graph_xack_only_after_correlated_async_xadd(broker, resources):
    incoming, outgoing = resources.key(), resources.key()
    c = await resources.client()
    f, h, consumer = source(broker, incoming, SourceHandler("callback"), result=True)
    p, _sh, publisher = sink(broker, outgoing, createStream=True)
    outcomes = []

    async def completed(delivery):
        outcomes.append((delivery, stream_id_from_context()))
        assert delivery.outcome == "published"
        await f.stream.result_consumer.consume(delivery)

    publisher.collect = completed
    consumer.sc.collect = publisher.consume
    try:
        await f.sources[1].start(Context())
        await p.sinks[1].start(Context())
        await c.xadd(incoming, {"event": "one", "x-stream-id": "graph-correlation"})
        await until(lambda: h.ended)
        assert h.ended == [None] and outcomes[0][1] == "graph-correlation"
        records = await c.xrange(outgoing)
        assert records[0][0].decode() == outcomes[0][0].id
        assert records[0][1][b"event"] == b"one"
        assert (await c.xpending(incoming, "workers"))["pending"] == 0
    finally:
        await stop(f)
        await stop(p)


@pytest.mark.asyncio
async def test_post_wire_context_cancel_stays_unknown(broker, resources):
    key = resources.key()
    c = await resources.client()
    async with Proxy(broker) as proxy, native(proxy.url, key, createStream=True) as s:
        ctx = Context()
        with request_context_scope(ctx):
            proxy.drop = True
            task = asyncio.create_task(s.publish(((b"a", b"b"),)))
            async with asyncio.timeout(2):
                while not await c.exists(key):
                    await asyncio.sleep(0.01)
            ctx.cancel()
            assert (await task).outcome == "unknown"
        proxy.drop = False
        await until(lambda: s.ready)
        assert await c.xlen(key) == 1


@pytest.mark.asyncio
async def test_begin_failure_skips_consume_and_end_then_consume_failure_ends_once(
    broker, resources
):
    key = resources.key()
    c = await resources.client()

    class H(SourceHandler):
        async def begin_request(self, sc):
            raise ValueError("begin failure")

    f, h, _ = source(broker, key, H())
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"a": "b"})
        await until(lambda: h.errors)
        assert not h.messages and not h.ended
    finally:
        await stop(f)

    class Broken(SourceHandler):
        async def consume_message(self, *args):
            raise ValueError("consume failure")

    g = setup(
        broker,
        streamKey=key,
        consumerGroup="workers",
        claimIdleMs=1,
        claimIntervalMs=10,
        readBlockMs=10,
    )
    h = Broken()
    source_factory(g.stream, h)
    try:
        await g.sources[1].start(Context())
        await until(lambda: h.ended)
        await g.sources[1].stop(Context(timedelta(seconds=1)))
        assert len(h.ended) == 1 and str(h.ended[0]) == "consume failure"
        assert (await c.xpending(key, "workers"))["pending"] == 1
    finally:
        await stop(g)


@pytest.mark.asyncio
async def test_case_insensitive_trace_fields_and_caller_retained(broker, resources):
    from opentelemetry import trace

    from pyservicelib_gorundebug.runtime.redisstreams import incoming_headers

    carrier = {
        "TraceParent": "00-12345678901234567890123456789012-1234567890123456-01",
        "Baggage": "test=value",
        "X-Stream-ID": "correlation",
    }
    with incoming_headers(carrier):
        assert trace.get_current_span().get_span_context().trace_id == int(
            "12345678901234567890123456789012", 16
        )
        with request_context_scope(Context()):
            with_stream_id("correlation")
            fields, _ = snapshot(
                [("TRACEPARENT", "old"), ("caller", "kept"), ("X-Stream-ID", "wrong")]
            )
    mapping = dict(fields)
    assert mapping[b"traceparent"].decode() == carrier["TraceParent"]
    assert b"TRACEPARENT" not in mapping and b"X-Stream-ID" not in mapping
    assert mapping[b"caller"] == b"kept" and mapping[b"x-stream-id"] == b"correlation"
    key = resources.key()
    async with native(broker, key, createStream=True) as s:
        assert (await s.publish(fields)).outcome == "published"
    c = await resources.client()
    assert (await c.xrange(key))[0][1] == mapping


@pytest.mark.asyncio
async def test_sync_and_skip_sink_results_do_not_confuse_publication(broker, resources):
    key = resources.key()
    c = await resources.client()
    f, _h, p = sink(broker, key, SinkHandler(sync=True), createStream=True)
    try:
        await f.sinks[1].start(Context())
        await p.consume([("a", "b")])
        assert f.results[0][0].outcome == "published" and await c.xlen(key) == 1
    finally:
        await stop(f)

    class Skip(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            await msg.skip("local-result")

    f, _h, p = sink(broker, key, Skip())
    try:
        await f.sinks[1].start(Context())
        await p.consume([])
        assert f.results[0][0] == "local-result" and await c.xlen(key) == 1
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_tls_client_certificate_and_hostname_failure(broker, resources, tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    await asyncio.to_thread(
        subprocess.run,
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=redis-test",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    tls.load_verify_locations(cert)
    tls.verify_mode = ssl.CERT_REQUIRED
    async with Proxy(broker, tls) as proxy:
        f = setup(proxy.url, streamKey=resources.key(), createStream=True)
        f.cfg.tls_ca_file = str(cert)
        f.cfg = f.cfg.model_copy(
            update={"tls_cert_file": str(cert), "tls_key_file": str(key)}
        )
        s = Session(f.cfg, f.ep, source=False, report=report)
        try:
            await s.start(Context())
            assert (await s.publish(((b"mtls", b"verified"),))).outcome == "published"
        finally:
            await s.close(Context())
        # Same trusted certificate, wrong requested DNS name: verification still fails.
        f.cfg.servers = [proxy.url.replace("127.0.0.1", "localhost")]
        bad = Session(f.cfg, f.ep, source=False, report=report)
        with pytest.raises(TransportStopped):
            await bad.start(Context())


@pytest.mark.asyncio
async def test_lost_read_reply_populates_pel_then_reclaims(broker, resources):
    key = resources.key()
    c = await resources.client()
    async with Proxy(broker) as proxy:
        f = setup(
            proxy.url,
            streamKey=key,
            consumerGroup="workers",
            createConsumerGroup=True,
            readBlockMs=20,
            claimIdleMs=10,
            claimIntervalMs=20,
        )
        f.cfg.operation_timeout_ms = 75
        s = Session(f.cfg, f.ep, source=True, report=report)
        try:
            await s.start(Context())
            await c.xadd(key, {"a": "b"})
            proxy.drop = True
            with pytest.raises((TimeoutError, TransportStopped)):
                await s.read(1, 20, Context())
            assert (await c.xpending(key, "workers"))["pending"] == 1
            proxy.drop = False
            await until(lambda: s.ready)
            _, entries, _ = await s.claim("0-0", 1, Context())
            assert len(entries) == 1 and entries[0].reclaimed
            assert await s.ack(entries[0].id, s.epoch, Context()) == 1
        finally:
            await s.close(Context())


@pytest.mark.asyncio
async def test_async_result_cancel_preserves_cause_and_never_acks(broker, resources):
    key = resources.key()
    c = await resources.client()

    class H(SourceHandler):
        async def consume_message(self, sc, state, msg, result):
            result.cancel(ValueError("business failed"))

    f, h, _ = source(broker, key, H(), result=True)
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"a": "b"})
        await until(lambda: h.ended)
        assert str(h.ended[0]) == "business failed"
        assert (await c.xpending(key, "workers"))["pending"] == 1
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_send_sync_cancel_after_admission_returns_unknown(broker, resources):
    key = resources.key()
    c = await resources.client()
    outcomes = []

    class H(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            msg.fields = value
            outcomes.append(await msg.send_sync())

    async with Proxy(broker) as proxy:
        f, h, p = sink(proxy.url, key, H(), createStream=True)
        try:
            await f.sinks[1].start(Context())
            ctx = Context()
            with request_context_scope(ctx):
                proxy.drop = True
                task = asyncio.create_task(p.consume([("a", "b")]))
                async with asyncio.timeout(2):
                    while not await c.exists(key):
                        await asyncio.sleep(0.01)
                ctx.cancel()
                await task
            assert outcomes[0].outcome == "unknown"
            assert isinstance(h.ended[0], TransportStopped)
            assert await c.xlen(key) == 1
        finally:
            proxy.drop = False
            await stop(f)


@pytest.mark.asyncio
@pytest.mark.parametrize("during_drain", [False, True])
async def test_idle_control_reconnect_fences_handle_even_during_drain(
    broker, resources, during_drain
):
    key = resources.key()
    c = await resources.client()
    f, h, consumer = source(broker, key, SourceHandler("hold"))
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"a": "b"})
        await until(lambda: h.messages)
        session = consumer.connection
        connection = await session.control.get_connection()
        try:
            await connection.send_command("CLIENT", "ID")
            identity = await connection.read_response()
        finally:
            await session.control.release(connection)
        if during_drain:
            consumer.stop_admission()
        # Kill only this endpoint's exact native connection, never the server or other clients.
        assert await c.execute_command("CLIENT", "KILL", "ID", identity) == 1
        await asyncio.sleep(0.02)
        with pytest.raises(TransportStopped):
            await h.messages[0].ack()
        assert session.epoch != h.messages[0].epoch
        assert (await c.xpending(key, "workers"))["pending"] == 1
    finally:
        h.gate.set()
        await stop(f)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["identity", "callback"])
async def test_async_result_error_reaches_end_without_ack(broker, resources, failure):
    key = resources.key()
    c = await resources.client()

    class H(SourceHandler):
        def get_message_id(self, sc, state, value):
            if failure == "identity":
                raise ValueError("identity failure")
            return "event"

        async def consume_message(self, sc, state, msg, result):
            async def callback(sc, state, value):
                raise ValueError("callback failure")

            result.set_result_callback("event", callback)
            await sc.collect(msg.data)

    f, h, consumer = source(broker, key, H(), result=True)
    consumer.sc.collect = f.stream.result_consumer.consume
    try:
        await f.sources[1].start(Context())
        await c.xadd(key, {"a": "b"})
        await until(lambda: h.ended)
        assert str(h.ended[0]) == failure + " failure"
        assert (await c.xpending(key, "workers"))["pending"] == 1
    finally:
        await stop(f)


@pytest.mark.asyncio
async def test_retained_sink_message_cannot_borrow_another_request_admission(
    broker, resources
):
    saved = []

    class H(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            if not saved:
                saved.append(msg)
                return
            saved[0].fields = [("stale", "value")]
            with pytest.raises(AdmissionRejected):
                saved[0].send(lambda d: d)
            msg.fields = [("fresh", "value")]
            msg.send(lambda d: d)

    key = resources.key()
    c = await resources.client()
    f, h, p = sink(broker, key, H(), createStream=True)
    try:
        await f.sinks[1].start(Context())
        await p.consume([])
        await p.consume([])
        await until(lambda: f.results)
        records = await c.xrange(key)
        assert len(records) == 1 and records[0][1][b"fresh"] == b"value"
        assert h.ended == [None, None]
    finally:
        await stop(f)

"""Real official RabbitMQ 4.x acceptance; never restart the shared broker."""

import asyncio
import os
import uuid
import signal
import time
from concurrent.futures import ThreadPoolExecutor
import rabbitmq_amqp_python_client as sdk
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
import pytest_asyncio
from rabbitmq_amqp_python_client import Message
from pyservicelib_gorundebug.runtime.config import (
    RabbitMQDataConnectorConfig,
    RabbitMQEndpointConfig,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import (
    stream_id_from_context,
    with_stream_id,
    request_context_scope,
)
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as RecordingMetrics
from pyservicelib_gorundebug.runtime.rabbitmq import (
    AdmissionRejected,
    TransportStopped,
    Session,
)
from pyservicelib_gorundebug.datasource.rabbitmq import (
    make_rabbitmq_endpoint_consumer as source_factory,
)
from pyservicelib_gorundebug.datasink.rabbitmq import (
    make_rabbitmq_endpoint_consumer_with_result as sink_factory,
)


@pytest.fixture
def broker():
    url = os.environ.get("RABBITMQ_TEST_URL")
    if not url:
        pytest.skip("set RABBITMQ_TEST_URL for real RabbitMQ 4.x acceptance")
    return url


async def until(predicate, timeout=6):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


RESOURCES = set()


def setup(url, *, result=False, **endpoint_options):
    cfg = RabbitMQDataConnectorConfig(
        id=1,
        name="broker",
        servers=[url],
        username="adapter",
        password="adapter-test",
        connectTimeoutMs=4000,
        operationTimeoutMs=1500,
    )
    values = dict(
        id=2,
        name="events",
        idDataConnector=1,
        queueName="py-adapter-" + uuid.uuid4().hex,
        createQueue=True,
        enabled=True,
        functionName="Handle",
    )
    values.update(endpoint_options)
    ep = RabbitMQEndpointConfig(**values)
    if ep.queue_name:
        RESOURCES.add(("queue", ep.queue_name))
    if ep.create_exchange:
        RESOURCES.add(("exchange", ep.exchange_name))
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
    def __init__(self, mode="accept"):
        self.mode = mode
        self.messages = []
        self.errors = []
        self.ended = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def concurrency(self, sc):
        return 1

    async def on_error(self, sc, error):
        self.errors.append(error)

    async def begin_request(self, sc):
        return "state"

    async def consume_message(self, sc, state, msg, result):
        self.messages.append(msg)
        self.entered.set()
        if self.mode == "wait":
            await self.release.wait()
        if self.mode == "callback":

            async def callback(sc, state, value):
                await msg.accept()
                result.done()
                return True

            result.set_result_callback("id", callback)
            await sc.collect(msg.data)
        else:
            if self.mode in ("accept", "wait"):
                await msg.accept()
            elif self.mode == "release" and len(self.messages) == 1:
                await msg.release()
            elif self.mode == "release":
                await msg.accept()
            elif self.mode == "reject":
                await msg.reject()
            result.done()

    def get_message_id(self, sc, state, value):
        return "id"

    async def end_request(self, sc, error, state):
        self.ended.append(error)


class SinkHandler:
    def __init__(self, sync=False):
        self.sync = sync
        self.ended = []

    def get_stream_id(self, value):
        return "publication"

    def begin_request(self, stream):
        return "state"

    async def consume_message(self, stream, state, value, msg):
        msg.message = Message(
            body=value,
            inferred=True,
            application_properties={"application": "kept"},
            correlation_id="native-correlation",
        )
        if self.sync:
            await msg.out(await msg.send_sync())
        else:
            msg.send(lambda delivery: delivery)

    async def end_request(self, stream, error, state):
        self.ended.append(error)


async def stop_all(f):
    ctx = Context(timedelta(seconds=2))
    for source in f.sources.values():
        await source.stop(ctx)
    for sink in f.sinks.values():
        await sink.stop(ctx)
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_native_async_and_explicit_accept(broker):
    f = setup(broker)
    h = SourceHandler()
    source_factory(f.stream, h)
    sh = SinkHandler()
    p = sink_factory(f.stream, sh)
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"hello")
        try:
            await until(lambda: f.results and h.ended)
        except TimeoutError:
            pytest.fail(
                str(
                    (
                        sh.ended,
                        f.results,
                        h.ended,
                        h.errors,
                        f.env.log.mock_calls,
                        [
                            (
                                t.exception()
                                if t.done() and not t.cancelled()
                                else "running"
                            )
                            for t in f.sources[1].get_endpoint(2).consumer.workers
                        ],
                    )
                )
            )
        assert f.results[0][0].outcome == "accepted"
        assert h.ended == [None]
        m = h.messages[0]
        assert m.data == b"hello" and m.message.correlation_id == "native-correlation"
        assert (
            m.headers["application"] == "kept"
            and m.headers["x-stream-id"] == "publication"
        )
        with pytest.raises(TransportStopped):
            await m.accept()
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_native_release_and_reject(broker):
    for mode in ("release", "reject"):
        f = setup(broker)
        f.env.metrics = RecordingMetrics()
        h = SourceHandler(mode)
        source_factory(f.stream, h)
        p = sink_factory(f.stream, SinkHandler())
        try:
            await f.sources[1].start(Context())
            await f.sinks[1].start(Context())
            await p.consume(b"item")
            await until(lambda: len(h.ended) == (2 if mode == "release" else 1))
            assert all(e is None for e in h.ended)
            # AMQP released leaves delivery-count unchanged, even on repeat receipt.
            labels = {"protocol": "rabbitmq", "connector": f.cfg.name, "endpoint": f.ep.name}
            for observation, count in [("received", 2 if mode == "release" else 1), ("redelivered", 0)]:
                assert f.env.metrics.counter("broker_endpoint_source_observations_total", {**labels, "observation": observation}).count() == count
        finally:
            await stop_all(f)


@pytest.mark.asyncio
async def test_native_result_before_emit_done_is_not_ack(broker):
    f = setup(broker, result=True)
    h = SourceHandler("callback")
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler(True))

    async def collect(value):
        await f.stream.result_consumer.consume(value)

    f.stream.consume = collect
    # StreamContext captures CollectFunc during construction; set before factory.
    f.sources.clear()
    source_factory(f.stream, h)
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"callback")
        await until(lambda: h.ended)
        assert h.ended == [None]
    finally:
        await stop_all(f)


@pytest_asyncio.fixture(autouse=True, loop_scope="function")
async def cleanup(broker):
    # A single test-owner thread serializes its independent official admin client.
    executor = ThreadPoolExecutor(max_workers=1)
    loop = asyncio.get_running_loop()

    def connect():
        from pyservicelib_gorundebug.runtime.rabbitmq import _connect_native

        return _connect_native(
            dict(
                servers=[broker],
                username="adapter",
                password="adapter-test",
                virtual_host="/",
                tls_enabled=False,
                tls_ca_file=None,
                tls_cert_file=None,
                tls_key_file=None,
            ),
            0,
        )

    admin = await loop.run_in_executor(executor, connect)

    async def call(fn):
        return await loop.run_in_executor(executor, lambda: fn(admin))

    yield call

    def close():
        for kind, name in tuple(RESOURCES):
            try:
                if kind == "queue":
                    admin.management().delete_queue(name)
                else:
                    admin.management().delete_exchange(name)
            except BaseException:
                pass
        RESOURCES.clear()
        admin.close()

    await loop.run_in_executor(executor, close)
    executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_done_never_settles_and_old_handle_fenced(broker):
    f = setup(broker)
    h = SourceHandler("done")
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"redeliver")
        await until(lambda: h.ended)
        await f.sources[1].stop(Context(timedelta(seconds=2)))
        with pytest.raises(TransportStopped):
            await h.messages[0].accept()
        again = setup(broker, queueName=f.ep.queue_name, createQueue=False)
        h2 = SourceHandler()
        source_factory(again.stream, h2)
        try:
            await again.sources[1].start(Context())
            await until(lambda: h2.ended)
            assert h2.messages[0].data == b"redeliver" and h2.ended == [None]
        finally:
            await stop_all(again)
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_unroutable_and_rejected_are_distinct(broker, cleanup):
    f = setup(
        broker,
        queueName=None,
        exchangeName="py-exchange-" + uuid.uuid4().hex,
        createQueue=False,
        createExchange=True,
    )
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sinks[1].start(Context())
        await p.consume(b"unbound")
        await until(lambda: f.results)
        assert f.results[0][0].outcome == "unroutable" and f.results[0][0].error is None
    finally:
        await stop_all(f)
    f = setup(broker, createQueue=False)
    await cleanup(
        lambda admin: admin.management().declare_queue(
            sdk.QuorumQueueSpecification(
                f.ep.queue_name, max_len=1, overflow_behaviour="reject-publish"
            )
        )
    )
    p = sink_factory(f.stream, SinkHandler(True))
    try:
        await f.sinks[1].start(Context())
        for i in range(12):
            await p.consume(b"full")
            if f.results[-1][0].outcome == "rejected":
                break
            await asyncio.sleep(0.03)
        delivery = f.results[-1][0]
        assert delivery.outcome == "rejected" and delivery.reason
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_missing_resources_and_wrong_queue_kind(broker, cleanup):
    f = setup(broker, createQueue=False)
    source_factory(f.stream, SourceHandler())
    try:
        with pytest.raises(TransportStopped):
            await f.sources[1].start(Context())
        response = await cleanup(
            lambda admin: admin.management().request(
                None,
                sdk.AddressHelper.queue_address(f.ep.queue_name),
                "GET",
                [200, 404],
            )
        )
        assert int(response.subject) == 404
    finally:
        await stop_all(f)
    await cleanup(
        lambda admin: admin.management().declare_queue(
            sdk.ClassicQueueSpecification(f.ep.queue_name)
        )
    )
    f.ep.queue_type = "quorum"
    g = setup(broker, queueName=f.ep.queue_name, queueType="quorum", createQueue=True)
    source_factory(g.stream, SourceHandler())
    try:
        with pytest.raises(TransportStopped):
            await g.sources[1].start(Context())
    finally:
        await stop_all(g)


@pytest.mark.asyncio
async def test_async_admission_deadline_unknown_and_recovery(broker):
    f = setup(broker, publisherMaxInFlight=1)
    h = SinkHandler()
    p = sink_factory(f.stream, h)
    try:
        await f.sinks[1].start(Context())
        session = f.sinks[1].get_endpoint(2).session
        old_pid = session.process.pid
        os.kill(old_pid, signal.SIGSTOP)  # Only this adapter owner, never the broker.
        started = time.monotonic()
        await p.consume(b"pending")
        assert time.monotonic() - started < 0.2 and h.ended == [None] and not f.results
        await p.consume(b"overflow")
        assert isinstance(h.ended[-1], AdmissionRejected)
        await until(lambda: f.results)
        assert f.results[0][0].outcome == "unknown"
        await until(lambda: session.ready.is_set() and session.process.pid != old_pid)
        await p.consume(b"recovered")
        await until(lambda: len(f.results) == 2)
        assert f.results[-1][0].outcome == "accepted"
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_stop_drains_admitted_source_settlement(broker):
    f = setup(broker)
    h = SourceHandler("wait")
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"drain")
        await h.entered.wait()
        stopped = asyncio.create_task(f.sources[1].stop(Context(timedelta(seconds=2))))
        await asyncio.sleep(0.05)
        assert not stopped.done()
        h.release.set()
        await stopped
        assert h.ended == [None]
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_stop_deadline_cancels_native_owner(broker):
    f = setup(broker)
    p = sink_factory(f.stream, SinkHandler())
    await f.sinks[1].start(Context())
    session = f.sinks[1].get_endpoint(2).session
    process = session.process
    os.kill(process.pid, signal.SIGSTOP)
    await p.consume(b"pending")
    started = time.monotonic()
    await f.sinks[1].stop(Context(timedelta(milliseconds=60)))
    assert time.monotonic() - started < 0.3
    await until(lambda: session.process is None)
    await until(lambda: f.results)
    assert f.results[0][0].outcome == "unknown"


@pytest.mark.asyncio
async def test_existing_binding_and_explicit_creation(broker):
    f = setup(
        broker,
        exchangeName="py-exchange-" + uuid.uuid4().hex,
        createExchange=True,
        createBinding=True,
        routingKey="bound",
    )
    h = SourceHandler()
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"bound")
        await until(lambda: h.ended)
        assert h.messages[0].data == b"bound" and f.results[0][0].outcome == "accepted"
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_source_recovery_invalidates_old_delivery(broker):
    f = setup(broker, result=True)
    h = SourceHandler("wait")
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"recover")
        await h.entered.wait()
        session = f.sources[1].get_endpoint(2).consumer.connection
        old = h.messages[0]
        old_pid = session.process.pid
        session.process.kill()  # Failure limited to this adapter's SDK owner.
        await until(lambda: session.ready.is_set() and session.process.pid != old_pid)
        with pytest.raises(TransportStopped):
            await old.accept()
        h.mode = "accept"
        h.release.set()
        await until(lambda: len(h.messages) >= 2 and len(h.ended) >= 2)
        assert h.messages[-1].data == b"recover"
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_stop_deadline_bounds_blocked_callback_and_capacity(broker):
    f = setup(broker, publisherMaxInFlight=1)
    entered = asyncio.Event()
    release = asyncio.Event()

    class Handler(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            msg.message = Message(body=value)

            async def callback(delivery):
                entered.set()
                await release.wait()
                return delivery

            msg.send(callback)

    h = Handler()
    p = sink_factory(f.stream, h)
    try:
        await f.sinks[1].start(Context())
        await p.consume(b"callback")
        await entered.wait()
        assert h.ended == [None]
        await p.consume(b"overflow")
        assert isinstance(h.ended[-1], AdmissionRejected)
        start = time.monotonic()
        await f.sinks[1].stop(Context(timedelta(milliseconds=50)))
        assert time.monotonic() - start < 0.3
        release.set()
        await until(lambda: f.results)
    finally:
        release.set()
        await stop_all(f)


@pytest.mark.asyncio
async def test_auth_failure_and_cancelled_start_are_bounded(broker):
    f = setup(broker)
    f.cfg.password = "not-the-password"
    p = sink_factory(f.stream, SinkHandler())
    started = time.monotonic()
    try:
        with pytest.raises((TransportStopped, TimeoutError)):
            await f.sinks[1].start(Context(timedelta(milliseconds=100)))
        assert time.monotonic() - started < 0.4
        await until(lambda: f.sinks[1].get_endpoint(2).session.process is None)
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_source_stop_bounds_cancellation_resistant_handler(broker):
    f = setup(broker)
    release = asyncio.Event()

    class Resistant(SourceHandler):
        async def consume_message(self, sc, state, msg, result):
            self.entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

    h = Resistant()
    source_factory(f.stream, h)
    p = sink_factory(f.stream, SinkHandler())
    try:
        await f.sources[1].start(Context())
        await f.sinks[1].start(Context())
        await p.consume(b"blocked")
        await h.entered.wait()
        start = time.monotonic()
        await f.sources[1].stop(Context(timedelta(milliseconds=50)))
        assert time.monotonic() - start < 0.3 and not h.ended
        release.set()
        await until(lambda: h.ended)
    finally:
        release.set()
        await stop_all(f)


@pytest.mark.asyncio
async def test_source_native_credit_and_unsettled_bound(broker):
    source = setup(broker, consumerCredits=1)
    sink = setup(broker, queueName=source.ep.queue_name, createQueue=False)
    h = SourceHandler("done")
    source_factory(source.stream, h)
    p = sink_factory(sink.stream, SinkHandler(True))
    try:
        await source.sources[1].start(Context())
        await sink.sinks[1].start(Context())
        await p.consume(b"first")
        await until(lambda: h.ended)
        await p.consume(b"second")
        await asyncio.sleep(0.1)
        assert len(h.messages) == 1  # Done did not settle or replenish native credit.
    finally:
        await stop_all(source)
        await stop_all(sink)


@pytest.mark.asyncio
async def test_publish_snapshot_and_byte_rejection(broker):
    f = setup(broker)
    native = []

    class Handler(SinkHandler):
        async def consume_message(self, stream, state, value, msg):
            msg.message = Message(body=value, application_properties={"caller": "kept"})
            native.append(msg.message)
            msg.send(lambda delivery: delivery)
            assert msg.message.application_properties == {"caller": "kept"}

    h = Handler()
    p = sink_factory(f.stream, h)
    try:
        await f.sinks[1].start(Context())
        await p.consume(b"x" * (1024 * 1024 + 1))
        assert isinstance(h.ended[-1], AdmissionRejected) and not f.results
        await p.consume(b"valid")
        native[-1].body = b"changed"
        await until(lambda: f.results)
        assert f.results[0][0].outcome == "accepted"
        source = setup(broker, queueName=f.ep.queue_name, createQueue=False)
        reader = SourceHandler()
        source_factory(source.stream, reader)
        try:
            await source.sources[1].start(Context())
            await until(lambda: reader.ended)
            assert reader.messages[0].data == b"valid"
        finally:
            await stop_all(source)
    finally:
        await stop_all(f)


@pytest.mark.asyncio
async def test_native_tls_verification(broker, tmp_path):
    import ssl
    import shutil
    import subprocess
    from urllib.parse import urlsplit

    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("TLS native acceptance requires openssl")
    certificate, key = tmp_path / "server.pem", tmp_path / "server.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(certificate, key)
    upstream = urlsplit(broker)
    bridges = set()

    async def bridge(reader, writer):
        task = asyncio.current_task()
        bridges.add(task)
        remote_writer = None
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                upstream.hostname, upstream.port or 5672
            )

            async def copy(reader, writer):
                while data := await reader.read(65536):
                    writer.write(data)
                    await writer.drain()

            tasks = [
                asyncio.create_task(copy(reader, remote_writer)),
                asyncio.create_task(copy(remote_reader, writer)),
            ]
            try:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            writer.close()
            if remote_writer:
                remote_writer.close()
            bridges.discard(task)

    server = await asyncio.start_server(bridge, "127.0.0.1", 0, ssl=tls)
    url = f"amqps://localhost:{server.sockets[0].getsockname()[1]}"
    try:
        f = setup(url)
        f.cfg.tls_ca_file = str(certificate)
        p = sink_factory(f.stream, SinkHandler(True))
        try:
            await f.sinks[1].start(Context())
            await p.consume(b"TLS verified")
            assert f.results[-1][0].outcome == "accepted"
        finally:
            await stop_all(f)
        # Same self-signed peer without the configured trust anchor must fail.
        g = setup(url)
        sink_factory(g.stream, SinkHandler())
        try:
            with pytest.raises((TransportStopped, TimeoutError)):
                await g.sinks[1].start(Context(timedelta(seconds=2)))
        finally:
            await stop_all(g)
    finally:
        server.close()
        await server.wait_closed()
        for task in list(bridges):
            task.cancel()
        await asyncio.gather(*list(bridges), return_exceptions=True)

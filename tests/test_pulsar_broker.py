"""Whole native block: unique namespace, explicit policies, no shared broker mutation."""

import asyncio
import os
import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pulsar.asyncio as native
import pytest
import pytest_asyncio

from pyservicelib_gorundebug.datasink.pulsar import (
    make_pulsar_endpoint_consumer_with_result as sink_factory,
)
from pyservicelib_gorundebug.datasource.pulsar import (
    make_pulsar_endpoint_consumer as source_factory,
)
from pyservicelib_gorundebug.runtime.config import PulsarDataConnectorConfig as DC
from pyservicelib_gorundebug.runtime.config import PulsarEndpointConfig as EP
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as RecordingMetrics
from pyservicelib_gorundebug.runtime.pulsar import (
    AdminError,
    AdmissionRejected,
    ProducerMessage,
    Session,
    TransportStopped,
    ensure_resources,
    snapshot,
)


async def until(predicate, timeout=8):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest_asyncio.fixture(loop_scope="function")
async def broker():
    url = os.getenv("PULSAR_TEST_URL")
    if not url:
        pytest.skip("set PULSAR_TEST_URL and PULSAR_TEST_ADMIN_URL")
    admin = os.getenv("PULSAR_TEST_ADMIN_URL", "http://127.0.0.1:18080")
    ns = "public/py-" + uuid.uuid4().hex
    topics = []
    sessions = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as http:

        async def request(method, path, body=None):
            async with http.request(method, admin + path, json=body) as r:
                data = await r.read()
                assert r.status < 300 or (method == "DELETE" and r.status == 404), (
                    r.status,
                    path,
                )
                import json

                return json.loads(data) if data else None

        await request("PUT", "/admin/v2/namespaces/" + ns, {})
        for policy, key in [
            ("autoTopicCreation", "allowAutoTopicCreation"),
            ("autoSubscriptionCreation", "allowAutoSubscriptionCreation"),
        ]:
            await request(
                "POST", "/admin/v2/namespaces/" + ns + "/" + policy, {key: False}
            )

        def endpoint(**kw):
            topic = "persistent://" + ns + "/t" + uuid.uuid4().hex
            topics.append((topic, kw.get("partitions", 0)))
            return EP(
                id=2,
                name="e",
                idDataConnector=1,
                functionName="Handle",
                enabled=True,
                topic=topic,
                **kw,
            )

        cfg = DC(id=1, name="p", servers=[url], adminUrl=admin, operationTimeoutMs=2000)
        errors = []

        async def report(error):
            errors.append(error)

        def session(ep, source):
            s = Session(cfg, ep, source=source, report=report)
            sessions.append(s)
            return s

        yield SimpleNamespace(
            cfg=cfg,
            ep=endpoint,
            session=session,
            request=request,
            ns=ns,
            errors=errors,
            sessions=sessions,
        )
        for s in sessions:
            try:
                await s.close(Context(timedelta(seconds=5)))
            except TransportStopped as error:
                errors.append(error)
        for topic, count in topics:
            path = "/admin/v2/persistent/" + topic.removeprefix("persistent://")
            metadata = await request(
                "GET", path + "/partitions?checkAllowAutoCreation=false"
            )
            await request(
                "DELETE",
                path
                + ("/partitions" if metadata["partitions"] else "")
                + "?force=true",
            )
        await request("DELETE", "/admin/v2/namespaces/" + ns)


@pytest.mark.asyncio
@pytest.mark.parametrize("partitions", [0, 2])
async def test_creation_and_policy(broker, partitions):
    e = broker.ep(subscriptionName="s")
    with pytest.raises(AdminError):
        await ensure_resources(broker.cfg, e, True)
    e.create_topic = True
    e.partitions = partitions
    with pytest.raises(ValueError, match="subscription missing"):
        await ensure_resources(broker.cfg, e, True)
    e.create_subscription = True
    await ensure_resources(broker.cfg, e, True)
    e.create_topic = False
    e.partitions = 0
    e.create_subscription = False
    await ensure_resources(broker.cfg, e, True)
    await broker.request(
        "POST",
        "/admin/v2/namespaces/" + broker.ns + "/autoTopicCreation",
        {"allowAutoTopicCreation": True, "topicType": "non-partitioned"},
    )
    with pytest.raises(ValueError, match="explicitly disable"):
        await ensure_resources(broker.cfg, e, True)


@pytest.mark.asyncio
async def test_binary_ack_nack_replacement_and_cancel(broker):
    e = broker.ep(
        createTopic=True,
        subscriptionName="s",
        createSubscription=True,
        negativeAckRedeliveryDelayMs=100,
    )
    source = broker.session(e, True)
    await source.start(Context())
    sink = broker.session(
        EP(id=3, name="out", idDataConnector=1, functionName="Publish", topic=e.topic),
        False,
    )
    await sink.start(Context())
    publication, _ = snapshot(
        ProducerMessage(b"\x00\xff", {"custom": "v"}, "key", 1234)
    )
    result = await sink.publish(publication)
    assert result.outcome == "Accepted" and result.id
    message = await source.receive(Context(timedelta(seconds=5)))
    assert (
        message.data() == b"\x00\xff"
        and message.properties()["custom"] == "v"
        and message.partition_key() == "key"
        and message.event_timestamp() == 1234
    )
    assert message.message_id().serialize() == result.id.serialize()
    await source.settle(message, source.epoch, Context(), True)
    again = await source.receive(Context(timedelta(seconds=5)))
    assert again.message_id().serialize() == result.id.serialize()
    await source.close(Context())
    replacement = broker.session(e, True)
    await replacement.start(Context())
    pending = await replacement.receive(Context(timedelta(seconds=5)))
    await replacement.settle(pending, replacement.epoch, Context())
    with pytest.raises(TransportStopped):
        await source.settle(message, source.epoch, Context())
    ctx = Context()
    task = asyncio.create_task(replacement.receive(ctx))
    await asyncio.sleep(0.05)
    ctx.cancel()
    with pytest.raises(TransportStopped):
        await asyncio.wait_for(task, 1)


def graph(broker, ep):
    sources, sinks = {}, {}
    env = SimpleNamespace(
        config=SimpleNamespace(
            get_data_connector_by_id=lambda _: broker.cfg,
            get_endpoint_config_by_id=lambda _: ep,
        ),
        metrics=NoopMetrics(),
        log=Mock(),
        tracing=None,
    )
    env.get_datasource, env.get_datasink = sources.get, sinks.get
    env.add_datasource = lambda ds: sources.__setitem__(ds.id, ds)
    env.add_datasink = lambda ds: sinks.__setitem__(ds.id, ds)
    values = []

    async def collect(value):
        values.append(value)

    stream = SimpleNamespace(
        environment=env,
        endpoint_id=ep.id,
        name="flow",
        consume=collect,
        consume_result=collect,
        error_stream=SimpleNamespace(consume=collect),
        get_result_stream=lambda: object(),
        set_sink_consumer=lambda c: None,
        set_result_consumer=lambda c: setattr(stream, "result_consumer", c),
    )
    return SimpleNamespace(env=env, stream=stream, values=values)


@pytest.mark.asyncio
@pytest.mark.parametrize("ack", [False, True])
async def test_graph_correlation_done_is_not_ack(broker, ack):
    source_ep = broker.ep(
        createTopic=True, subscriptionName="graph", createSubscription=True
    )
    sink_ep = EP(
        id=3,
        name="out",
        idDataConnector=1,
        functionName="Publish",
        topic=source_ep.topic,
        enabled=True,
    )
    a, b = graph(broker, source_ep), graph(broker, sink_ep)
    a.env.metrics = RecordingMetrics()
    held = []
    ended = []

    class Input:
        def concurrency(self, sc):
            return 2

        async def on_error(self, sc, error):
            raise error

        async def begin_request(self, sc):
            return None

        async def consume_message(self, sc, state, msg, result):
            held.append(msg)

            async def callback(sc, state, value):
                if ack:
                    await msg.ack()
                result.done()
                return True

            result.set_result_callback("id", callback)
            # Synchronous result emission exercises callback-before-collect ordering.
            await a.stream.result_consumer.consume("id")

        def get_message_id(self, sc, state, value):
            return value

        async def end_request(self, sc, error, state):
            ended.append(error)

    class Output:
        def get_stream_id(self, value):
            return "event"

        def begin_request(self, stream):
            return None

        async def consume_message(self, stream, state, value, msg):
            msg.message = ProducerMessage(value, {"X-Trace": "1"})
            msg.send(lambda delivery: delivery)

        async def end_request(self, stream, error, state):
            assert error is None

    source = source_factory(a.stream, Input())
    sink = sink_factory(b.stream, Output())
    broker.sessions.extend([source.connection, sink.endpoint.session])
    await source.start(Context())
    await sink.endpoint.datasink.start(Context())
    await sink.consume(b"value")
    await until(lambda: ended and b.values)
    assert ended == [None] and b.values[0].outcome == "Accepted"
    labels = {"protocol": "pulsar", "connector": broker.cfg.name, "endpoint": source_ep.name}
    assert a.env.metrics.counter("broker_endpoint_source_observations_total", {**labels, "observation": "received"}).count() == 1
    assert a.env.metrics.counter("broker_endpoint_source_observations_total", {**labels, "observation": "redelivered"}).count() == 0
    assert {k.lower(): v for k, v in held[0].headers.items()}["x-stream-id"] == "event"
    assert {k.lower(): v for k, v in held[0].headers.items()}["x-trace"] == "1"
    assert held[0].message.message_id().serialize() == b.values[0].id.serialize()
    with pytest.raises(TransportStopped):
        await held[0].ack()
    await source.endpoint.datasource.stop(Context(timedelta(seconds=2)))
    await sink.endpoint.datasink.stop(Context(timedelta(seconds=2)))
    replacement = broker.session(source_ep, True)
    await replacement.start(Context())
    if ack:
        with pytest.raises(TimeoutError):
            await replacement.receive(Context(timedelta(seconds=0.3)))
    else:
        pending = await replacement.receive(Context(timedelta(seconds=3)))
        assert pending.data() == b"value"
        await replacement.settle(pending, replacement.epoch, Context())


@pytest.mark.asyncio
async def test_sink_callback_capacity_retirement_and_drain(broker):
    e = broker.ep(createTopic=True, publisherMaxInFlight=1)
    g = graph(broker, e)
    held = []
    entered = asyncio.Event()
    release = asyncio.Event()

    class Handler:
        def get_stream_id(self, value):
            return "id"

        def begin_request(self, stream):
            return None

        async def consume_message(self, stream, state, value, msg):
            held.append(msg)
            if value:
                msg.message = ProducerMessage(b"one")

                async def delivered(result):
                    entered.set()
                    await release.wait()
                    return result

                msg.send(delivered)

        async def end_request(self, stream, error, state):
            assert error is None

    sink = sink_factory(g.stream, Handler())
    broker.sessions.append(sink.endpoint.session)
    await sink.endpoint.datasink.start(Context())
    await sink.consume(False)  # retained, unsent handle
    await sink.consume(True)
    await asyncio.wait_for(entered.wait(), 5)
    with pytest.raises(AdmissionRejected):
        held[0].send(lambda d: d)
    with pytest.raises(AdmissionRejected):
        await held[0].send_sync()
    with pytest.raises(AdmissionRejected):
        sink.endpoint.submit(ProducerMessage(b"extra"), None, g.stream.consume)
    stop = asyncio.create_task(
        sink.endpoint.datasink.stop(Context(timedelta(seconds=2)))
    )
    await asyncio.sleep(0.05)
    assert not stop.done()
    release.set()
    await stop
    assert len(g.values) == 1 and g.values[0].outcome == "Accepted"


@pytest.mark.asyncio
async def test_two_consumers_and_out_of_order_individual_ack(broker):
    e = broker.ep(
        createTopic=True,
        subscriptionName="shared",
        createSubscription=True,
        receiverQueueSize=1,
    )
    a = broker.session(e, True)
    b = broker.session(e, True)
    await a.start(Context())
    await b.start(Context())
    s = broker.session(
        EP(id=3, name="s", idDataConnector=1, functionName="Send", topic=e.topic), False
    )
    await s.start(Context())
    for value in (b"first", b"second"):
        assert (await s.publish(ProducerMessage(value))).outcome == "Accepted"
    one, two = await asyncio.gather(
        a.receive(Context(timedelta(seconds=4))),
        b.receive(Context(timedelta(seconds=4))),
    )
    assert {one.data(), two.data()} == {b"first", b"second"}
    await b.settle(two, b.epoch, Context())
    await a.close(Context())
    await b.close(Context())
    c = broker.session(e, True)
    await c.start(Context())
    pending = await c.receive(Context(timedelta(seconds=4)))
    assert pending.message_id().serialize() == one.message_id().serialize()
    await c.settle(pending, c.epoch, Context())


@pytest.mark.asyncio
async def test_native_reply_loss_unknown_without_adapter_replay(broker):
    from tests.pulsar_proxy import PulsarProxy

    e = broker.ep(
        createTopic=True, subscriptionName="observer", createSubscription=True
    )
    source = broker.session(e, True)
    await source.start(Context())
    async with PulsarProxy(broker.cfg.servers[0]) as proxy:
        cfg = broker.cfg.model_copy(
            update={"servers": [proxy.url], "operation_timeout_ms": 300}
        )
        ep = EP(id=3, name="out", idDataConnector=1, functionName="Send", topic=e.topic)
        sink = Session(cfg, ep, source=False, report=AsyncMock())
        broker.sessions.append(sink)
        await sink.start(Context())
        proxy.drop_receipts = True
        result = await sink.publish(ProducerMessage(b"ambiguous"))
        assert result.outcome == "Unknown"
        message = await source.receive(Context(timedelta(seconds=2)))
        assert message.data() == b"ambiguous" and proxy.sends == 1
        await source.settle(message, source.epoch, Context())
        proxy.drop_receipts = False
        # Explicit next publication; the adapter never retries the ambiguous one.
        again = await sink.publish(ProducerMessage(b"next"))
        assert again.outcome == "Accepted" and proxy.sends == 2
        await sink.close(Context(timedelta(seconds=3)))


@pytest.mark.asyncio
async def test_stop_deadline_with_callback_and_repeated_native_close(broker):
    e = broker.ep(createTopic=True, publisherMaxInFlight=1)
    g = graph(broker, e)
    entered = asyncio.Event()
    release = asyncio.Event()

    class Handler:
        def get_stream_id(self, value):
            return "id"

        def begin_request(self, stream):
            return None

        async def consume_message(self, stream, state, value, msg):
            msg.message = ProducerMessage(b"held")

            async def callback(delivery):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    await release.wait()
                return delivery

            msg.send(callback)

        async def end_request(self, stream, error, state):
            pass

    sink = sink_factory(g.stream, Handler())
    broker.sessions.append(sink.endpoint.session)
    await sink.endpoint.datasink.start(Context())
    await sink.consume("value")
    await entered.wait()
    loop = asyncio.get_running_loop()
    start = loop.time()
    with pytest.raises(TransportStopped):
        await sink.endpoint.datasink.stop(Context(timedelta(seconds=0.05)))
    assert loop.time() - start < 0.5
    close_task = sink.endpoint.session.close_task
    assert close_task is not None
    release.set()
    await sink.endpoint.datasink.stop(Context(timedelta(seconds=2)))
    assert sink.endpoint.session.close_task is close_task and close_task.done()


@pytest.mark.asyncio
async def test_tls_native_identity_and_mtls(broker, tmp_path):
    import ssl
    import subprocess

    from aiohttp import web

    from tests.pulsar_proxy import PulsarProxy

    def certificate(name, san):
        cert, key = tmp_path / (name + ".crt"), tmp_path / (name + ".key")
        subprocess.run(
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
                "/CN=" + name,
                "-addext",
                "subjectAltName=DNS:" + san,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return cert, key

    cert, key = certificate("localhost", "localhost")
    wrong, wrongkey = certificate("wrong", "wrong.example")
    client, clientkey = certificate("client", "client")
    untrusted, untrustedkey = certificate("untrusted", "localhost")
    ca = tmp_path / "trusted.pem"
    ca.write_bytes(cert.read_bytes() + wrong.read_bytes())
    admin_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    admin_context.load_cert_chain(cert, key)
    async with aiohttp.ClientSession() as upstream:

        async def forward(request):
            async with upstream.request(
                request.method,
                broker.cfg.admin_url + request.path_qs,
                data=await request.read(),
                headers={"Content-Type": "application/json"},
                allow_redirects=False,
            ) as response:
                return web.Response(
                    status=response.status,
                    body=await response.read(),
                    content_type="application/json",
                )

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", forward)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=admin_context)
        await site.start()
        port = runner.addresses[0][1]
        e = broker.ep(createTopic=True)
        try:
            for name, server_cert, server_key, mtls in [
                ("valid", cert, key, False),
                ("wrong-host", wrong, wrongkey, False),
                ("mtls", cert, key, True),
                ("missing-identity", cert, key, True),
                ("untrusted", untrusted, untrustedkey, False),
            ]:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.load_cert_chain(server_cert, server_key)
                if mtls:
                    context.verify_mode = ssl.CERT_REQUIRED
                    context.load_verify_locations(client)
                async with PulsarProxy(broker.cfg.servers[0], context) as proxy:
                    cfg = DC(
                        id=1,
                        name="tls",
                        servers=[proxy.url],
                        adminUrl=f"https://localhost:{port}",
                        tlsCaFile=str(ca),
                        operationTimeoutMs=1000,
                        tlsCertFile=str(client) if name == "mtls" else None,
                        tlsKeyFile=str(clientkey) if name == "mtls" else None,
                    )
                    s = Session(cfg, e, source=False, report=AsyncMock())
                    broker.sessions.append(s)
                    if name in ("wrong-host", "missing-identity", "untrusted"):
                        with pytest.raises(
                            (TimeoutError, TransportStopped, native.PulsarException)
                        ):
                            await s.start(Context(timedelta(seconds=2)))
                    else:
                        await s.start(Context(timedelta(seconds=2)))
                        assert (
                            await s.publish(ProducerMessage(name.encode()))
                        ).outcome == "Accepted"
                    await s.close(Context(timedelta(seconds=2)))
        finally:
            await runner.cleanup()


@pytest.mark.asyncio
async def test_native_reconnect_after_connection_interruption(broker):
    from tests.pulsar_proxy import PulsarProxy

    e = broker.ep(createTopic=True)
    async with PulsarProxy(broker.cfg.servers[0]) as proxy:
        cfg = broker.cfg.model_copy(update={"servers": [proxy.url]})
        s = Session(cfg, e, source=False, report=AsyncMock())
        broker.sessions.append(s)
        await s.start(Context())
        assert (await s.publish(ProducerMessage(b"before"))).outcome == "Accepted"
        for writer in tuple(proxy.writers):
            writer.close()
        await asyncio.sleep(0.1)
        assert (await s.publish(ProducerMessage(b"after"))).outcome == "Accepted"
        assert proxy.sends == 2
        await s.close(Context(timedelta(seconds=3)))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["Shared", "Exclusive", "Failover"])
async def test_latest_creation_and_existing_cursor_not_reset(broker, mode):
    e = broker.ep(createTopic=True)
    producer = broker.session(e, False)
    await producer.start(Context())
    assert (await producer.publish(ProducerMessage(b"old"))).outcome == "Accepted"
    source_ep = EP(
        id=4,
        name="s",
        idDataConnector=1,
        functionName="Receive",
        topic=e.topic,
        subscriptionName="position",
        subscriptionType=mode,
        createSubscription=True,
        subscriptionInitialPosition="Latest",
    )
    source = broker.session(source_ep, True)
    await source.start(Context())
    assert (await producer.publish(ProducerMessage(b"new"))).outcome == "Accepted"
    received = await source.receive(Context(timedelta(seconds=3)))
    assert received.data() == b"new"
    await source.close(Context())
    replacement = broker.session(source_ep, True)
    await replacement.start(Context())
    pending = await replacement.receive(Context(timedelta(seconds=3)))
    assert pending.message_id().serialize() == received.message_id().serialize()
    await replacement.settle(pending, replacement.epoch, Context())

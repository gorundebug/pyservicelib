"""Shared isolated official fixture; never touches the shared broker."""

import json
import os
import time
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.runtime.config import (
    PulsarDataConnectorConfig as DC,
)
from pyservicelib_gorundebug.runtime.config import (
    PulsarEndpointConfig as EP,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.pulsar import ProducerMessage, Session


@pytest.fixture
def auth_manifest():
    path = os.getenv("PULSAR_AUTH_MANIFEST")
    if not path:
        pytest.skip("set PULSAR_AUTH_MANIFEST from isolated fixture")
    with open(path) as handle:
        return json.load(handle)


@pytest.mark.asyncio
async def test_native_admin_token_separation(auth_manifest):
    f = auth_manifest
    sessions = []

    def session(
        role,
        admin="tenant-admin",
        source=False,
        topic="persistent://acl/events/allowed",
    ):
        c = DC(
            id=11,
            name="auth",
            servers=[f["url"]],
            adminUrl=f["adminUrl"],
            token=f["tokens"][role],
            adminToken=f["tokens"].get(admin, admin),
            operationTimeoutMs=2000,
            connectTimeoutMs=1000,
        )
        ep = EP(
            id=1,
            name="auth",
            functionName="Handle",
            idDataConnector=11,
            topic=topic,
            **({"subscriptionName": "workers"} if source else {}),
        )
        s = Session(c, ep, source=source, report=AsyncMock())
        sessions.append(s)
        return s

    def ctx():
        return Context(timedelta(seconds=3))

    try:
        consumer, producer = session("consumer", source=True), session("producer")
        await consumer.start(ctx())
        await producer.start(ctx())
        result = await producer.publish(ProducerMessage(b"python-separated"))
        assert result.outcome == "Accepted"
        m = await consumer.receive(ctx())
        assert m.message_id().serialize() == result.id.serialize()
        await consumer.settle(m, consumer.epoch, ctx())
        for role, admin, source, topic in [
            ("producer", "tenant-admin", True, "events/allowed"),
            ("consumer", "tenant-admin", False, "events/allowed"),
            ("topic-producer", "tenant-admin", False, "events/forbidden"),
            ("producer", "tenant-admin", False, "denied/allowed"),
            ("admin", "outsider", False, "events/allowed"),
            ("admin", "invalid-admin-token", False, "events/allowed"),
            ("producer", "", False, "events/allowed"),
        ]:
            s = session(role, admin, source, "persistent://acl/" + topic)
            with pytest.raises(Exception) as caught:
                await s.start(ctx())
            assert all(token not in str(caught.value) for token in f["tokens"].values())
        started = time.monotonic()
        await consumer.close(ctx())
        await producer.close(ctx())
        assert time.monotonic() - started < 3
    finally:
        for s in sessions:
            await s.close(ctx())

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pulsar
import pulsar.asyncio as native
import pytest

from pyservicelib_gorundebug.runtime.config import PulsarDataConnectorConfig as DC
from pyservicelib_gorundebug.runtime.config import PulsarEndpointConfig as EP
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import request_context_scope
from pyservicelib_gorundebug.runtime.pulsar import (
    ProducerMessage,
    Session,
    delivery_error,
)


def session():
    cfg = DC(
        id=1,
        name="p",
        servers=["pulsar://localhost"],
        adminUrl="http://localhost",
        operationTimeoutMs=100,
    )
    ep = EP(
        id=2,
        name="p",
        idDataConnector=1,
        functionName="Publish",
        topic="persistent://public/test/t",
        publisherMaxInFlight=1,
    )
    result = Session(cfg, ep, source=False, report=AsyncMock())
    result.ready = True
    result.producer = SimpleNamespace(send=AsyncMock(return_value="native-id"))
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["invalid", "cancelled", "expired", "source", "closed", "capacity"]
)
async def test_before_handoff_is_not_sent(kind):
    s = session()
    ctx = Context()
    message = ProducerMessage(b"x")
    if kind == "invalid":
        message.data = object()
    if kind == "cancelled":
        ctx.cancel()
    if kind == "expired":
        ctx = Context(timedelta(seconds=-1))
    if kind == "source":
        s.source = True
    if kind == "closed":
        s.stopped = True
    if kind == "capacity":
        s.active.add(object())
    with request_context_scope(ctx):
        result = await s.publish(message)
    assert result.outcome == "NotSent" and result.error is not None
    s.producer.send.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [TimeoutError("deadline"), RuntimeError("native failure")]
)
async def test_after_handoff_is_unknown_with_original_cause(failure):
    s = session()
    s.producer.send.side_effect = failure
    result = await s.publish(ProducerMessage(b"x"))
    assert result.outcome == "Unknown" and result.error is failure
    s.producer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_after_handoff_keeps_native_owned():
    s = session()
    started = asyncio.Event()
    finish = asyncio.Event()

    async def send(*args, **kw):
        started.set()
        await finish.wait()
        return "id"

    s.producer.send.side_effect = send
    ctx = Context()
    with request_context_scope(ctx):
        task = asyncio.create_task(s.publish(ProducerMessage(b"x")))
    await started.wait()
    ctx.cancel()
    result = await task
    assert result.outcome == "Unknown" and len(s.active) == 1
    s.producer.send.assert_awaited_once()
    finish.set()
    await asyncio.gather(*s.active)


@pytest.mark.parametrize(
    "code,outcome",
    [
        (pulsar.Result.ProducerQueueIsFull, "NotSent"),
        (pulsar.Result.MessageTooBig, "NotSent"),
        (pulsar.Result.AuthorizationError, "Rejected"),
        (pulsar.Result.TopicTerminated, "Rejected"),
        (pulsar.Result.Timeout, "Unknown"),
        (pulsar.Result.ConnectError, "Unknown"),
        (pulsar.Result.NotConnected, "Unknown"),
        (pulsar.Result.AlreadyClosed, "Unknown"),
    ],
)
def test_native_outcomes(code, outcome):
    error = native.PulsarException(code)
    result = delivery_error(error)
    assert result.outcome == outcome and result.error is error


@pytest.mark.asyncio
async def test_sync_graph_wait_does_not_mask_definitive_not_sent():
    from pyservicelib_gorundebug.datasink.pulsar import SinkMessage
    from pyservicelib_gorundebug.runtime.pulsar import Delivery

    ctx = Context()
    reason = TimeoutError("before native handoff")

    async def completed():
        return Delivery("NotSent", reason)

    endpoint = SimpleNamespace(submit=lambda *args: asyncio.create_task(completed()))
    message = SinkMessage(endpoint, AsyncMock())
    ctx.cancel()
    # Admission is stubbed here; a completed native classification must not be
    # overwritten merely because the surrounding wait observes cancellation.
    with request_context_scope(ctx):
        result = await message.send_sync()
    assert result.outcome == "NotSent" and result.error is reason

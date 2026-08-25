from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from temporalio.worker import ExecuteActivityInput

from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.runtime.context import (
    request_deadline,
    request_priority,
    request_stream_id,
)
from pyservicelib_gorundebug.runtime.durable_context import (
    current_durable_call_context,
    durable_call_delay,
    durable_call_heartbeat,
)
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.runtime.environment.tracing import (
    Tracing,
    sampling_enabled,
    sampling_scope,
)
from pyservicelib_gorundebug.datasource.temporal.connector import (
    Connector,
    EndpointEnvelope,
    EndpointResult,
    _EndpointRegistration,
    _direct_workflow_type,
    _endpoint_workflow_id,
    _make_endpoint_activity,
    _make_endpoint_workflow,
    _schedule_workflow_id,
    _scheduled_time_nanos,
    _temporal_cron_expression,
    _validate_workflow_ownership,
)
from pyservicelib_gorundebug.datasource.temporal.context_propagation import (
    TEMPORAL_HEADER_DEADLINE_UNIX_NANO,
    TEMPORAL_HEADER_PRIORITY,
    TEMPORAL_HEADER_STREAM_ID,
    _ActivityInbound,
    _current_carrier,
    _encode_carrier,
)


def test_temporal_cron_preserves_portable_minute_semantics() -> None:
    assert _temporal_cron_expression("  */5   * * * * ") == "0 */5 * * * *"


def test_workflow_identity_uses_connector_endpoint_and_business_message_id() -> None:
    assert _endpoint_workflow_id(
        "Temporal Main", "Durable Job", "order/42:item 7"
    ) == "temporal_main/endpoint/durable_job/order%2F42%3Aitem%207"


@pytest.mark.asyncio
async def test_direct_workflow_endpoint_uses_durable_timer_and_noop_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waited: list[timedelta] = []

    async def sleep(duration: timedelta) -> None:
        waited.append(duration)

    monkeypatch.setattr(
        "pyservicelib_gorundebug.datasource.temporal.connector.workflow.sleep",
        sleep,
    )

    async def handler(envelope: EndpointEnvelope) -> EndpointResult:
        assert current_durable_call_context() is not None
        durable_call_heartbeat("ignored in Workflow")
        assert await durable_call_delay(timedelta(hours=1))
        return EndpointResult(payload=envelope.payload + b"-done")

    workflow_type = _direct_workflow_type("Temporal", "Workflow Job")
    workflow_class = _make_endpoint_workflow(
        _EndpointRegistration(
            12,
            "temporal.endpoint.workflow_job.v1",
            handler,
            workflow_type,
        )
    )
    result = await workflow_class().run(
        EndpointEnvelope(1, 12, "workflow-1", "request-1", 0, payload=b"job")
    )
    assert result.payload == b"job-done"
    assert waited == [timedelta(hours=1)]
    assert _schedule_workflow_id("Temporal Connector", "Workflow Job") == (
        "temporal_connector/schedule/workflow_job"
    )


class _Config:
    def __init__(self) -> None:
        self.connector = SimpleNamespace(
            id=7,
            name="Temporal Main",
            implementation=DataConnectorImplementation.TemporalPython,
        )
        self.endpoint = SimpleNamespace(
            id=11,
            name="Durable Job",
            id_data_connector=7,
            enabled=True,
            task_queue="automation",
            activity_start_to_close_timeout=1_000,
            activity_heartbeat_timeout=0,
            workflow_execution_timeout=10_000,
            maximum_attempts=3,
        )

    def get_data_connector_by_id(self, connector_id: int):
        assert connector_id == 7
        return self.connector

    def get_endpoint_config_by_id(self, endpoint_id: int):
        assert endpoint_id == 11
        return self.endpoint


class _Environment:
    def __init__(self) -> None:
        self.config = _Config()
        self.service_config = SimpleNamespace(id=1, name="Automation Service")
        self.metrics = NoopMetrics()
        self.log = SimpleNamespace(warn=lambda *_: None, error=lambda *_: None)


def test_scheduled_time_uses_temporal_schedule_workflow_id_suffix() -> None:
    fallback = datetime(2026, 8, 24, 12, 35, 1, tzinfo=timezone.utc)
    assert _scheduled_time_nanos(
        "temporal/schedule/durableJob-2026-08-24T12:30:00.123456789Z",
        fallback,
    ) == 1_787_574_600_123_456_789
    assert _scheduled_time_nanos("manual-workflow", fallback) == int(
        fallback.timestamp() * 1_000_000_000
    )


@pytest.mark.asyncio
async def test_on_demand_endpoint_runs_inside_activity_scope(monkeypatch) -> None:
    received: list[EndpointEnvelope] = []
    heartbeats: list[object] = []
    monkeypatch.setattr(
        "pyservicelib_gorundebug.datasource.temporal.connector.activity.heartbeat",
        heartbeats.append,
    )

    async def handler(envelope: EndpointEnvelope) -> EndpointResult:
        received.append(envelope)
        durable = current_durable_call_context()
        assert durable is not None
        assert durable.message_id == "item/42"
        durable_call_heartbeat("accepted")
        return EndpointResult(payload=b"accepted")

    function = _make_endpoint_activity(
        _EndpointRegistration(11, "temporal.endpoint.durable_job.v1", handler)
    )
    envelope = EndpointEnvelope(
        version=1,
        endpoint_id=11,
        message_id="item/42",
        stream_id="stream-1",
        priority=0,
        payload=b"value",
    )

    result = await function(envelope)

    assert received[0].scheduled is False
    assert result == EndpointResult(payload=b"accepted")
    assert heartbeats == ["accepted"]
    assert current_durable_call_context() is None


@pytest.mark.asyncio
async def test_endpoint_activity_propagates_business_error() -> None:
    async def handler(_envelope: EndpointEnvelope) -> EndpointResult:
        raise RuntimeError("business failure")

    function = _make_endpoint_activity(
        _EndpointRegistration(11, "temporal.endpoint.durable_job.v1", handler)
    )
    envelope = EndpointEnvelope(
        version=1,
        endpoint_id=11,
        message_id="job-2",
        stream_id="stream-2",
        priority=0,
        payload=b"value",
    )

    with pytest.raises(RuntimeError, match="business failure"):
        await function(envelope)


def test_temporal_header_deadline_is_an_absolute_timezone_independent_instant() -> None:
    deadline = datetime(2026, 8, 25, 15, 30, tzinfo=timezone(timedelta(hours=3)))
    stream_token = request_stream_id.set("stream-header")
    priority_token = request_priority.set(7)
    deadline_token = request_deadline.set(deadline)
    try:
        with sampling_scope(True):
            carrier = _current_carrier(None)
    finally:
        request_deadline.reset(deadline_token)
        request_priority.reset(priority_token)
        request_stream_id.reset(stream_token)

    assert carrier[TEMPORAL_HEADER_STREAM_ID] == "stream-header"
    assert carrier[TEMPORAL_HEADER_PRIORITY] == "7"
    assert carrier[TEMPORAL_HEADER_DEADLINE_UNIX_NANO] == str(
        int(datetime(2026, 8, 25, 12, 30, tzinfo=timezone.utc).timestamp() * 1e9)
    )


@pytest.mark.asyncio
async def test_temporal_activity_interceptor_extracts_native_headers() -> None:
    baggage: ContextVar[str] = ContextVar("temporal_test_baggage", default="")

    class _Tracing(Tracing):
        def tracer(self, name):  # type: ignore[no-untyped-def]
            del name
            return None

        @contextmanager
        def extract(self, carrier):  # type: ignore[no-untyped-def]
            token = baggage.set(carrier.get("baggage", ""))
            try:
                yield True
            finally:
                baggage.reset(token)

    class _Next:
        async def execute_activity(self, input):  # type: ignore[no-untyped-def]
            del input
            assert request_stream_id.get() == "activity-stream"
            assert request_priority.get() == 9
            assert request_deadline.get() == datetime(
                2026, 8, 25, 12, 30, tzinfo=timezone.utc
            )
            assert sampling_enabled()
            assert baggage.get() == "tenant=example"
            return "done"

    carrier = {
        "traceparent": (
            "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
        ),
        "baggage": "tenant=example",
        TEMPORAL_HEADER_STREAM_ID: "activity-stream",
        TEMPORAL_HEADER_PRIORITY: "9",
        TEMPORAL_HEADER_DEADLINE_UNIX_NANO: str(
            int(datetime(2026, 8, 25, 12, 30, tzinfo=timezone.utc).timestamp() * 1e9)
        ),
    }
    interceptor = _ActivityInbound(_Next(), _Tracing())  # type: ignore[arg-type]
    result = await interceptor.execute_activity(
        ExecuteActivityInput(
            fn=lambda: None,
            args=[],
            executor=None,
            headers=_encode_carrier(carrier),
        )
    )
    assert result == "done"
    assert request_stream_id.get() is None
    assert request_priority.get() is None
    assert request_deadline.get() is None


def test_remote_endpoint_activity_identity_uses_shared_connector_and_endpoint() -> None:
    connector = Connector(7, _Environment())  # type: ignore[arg-type]

    connector.register_endpoint_submission(11)

    assert connector._endpoints[11].activity_type == (
        "temporal_main.endpoint.durable_job.v1"
    )
    assert connector._endpoints[11].handler is None


@pytest.mark.asyncio
async def test_workflow_ownership_awaits_decoded_memo() -> None:
    class _Description:
        id = "workflow-1"
        workflow_type = "servicelib.test"

        async def memo(self) -> dict[str, str]:
            return {
                "servicelib.managedBy": "servicelib",
                "servicelib.owner": "owner-1",
                "servicelib.callId": "message-1",
            }

    class _Handle:
        async def describe(self) -> _Description:
            return _Description()

    await _validate_workflow_ownership(
        _Handle(), "servicelib.test", "owner-1", "message-1"
    )

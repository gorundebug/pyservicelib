from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from temporalio.converter import DataConverter
from temporalio.worker import ExecuteActivityInput

from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.runtime.common import DurableEnvelope
from pyservicelib_gorundebug.runtime.config import LinkId
from pyservicelib_gorundebug.runtime.durable_context import durable_call_success
from pyservicelib_gorundebug.runtime.context import (
    request_deadline,
    request_priority,
    request_stream_id,
)
from pyservicelib_gorundebug.runtime.environment.tracing import (
    Tracing,
    sampling_enabled,
    sampling_scope,
)
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.datasource.temporal.connector import (
    Connector,
    _DurableWorkflowRequest,
    _LinkRegistration,
    _make_link_activity,
    _scheduled_time_nanos,
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


class _Config:
    def __init__(self) -> None:
        self.connector = SimpleNamespace(
            id=7,
            name="temporal",
            implementation=DataConnectorImplementation.TemporalPython,
        )
        self.endpoint = SimpleNamespace(id=11, name="durableJob", id_data_connector=7)

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


def _envelope(*, to_id: int = 4) -> DurableEnvelope:
    return DurableEnvelope(
        version=1,
        from_id=3,
        to_id=to_id,
        call_id="call-1",
        stream_id="stream-1",
        priority=0,
        deadline_unix_nano=0,
        payload=b"value",
    )


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
async def test_temporal_activity_only_activates_registered_target() -> None:
    received: list[DurableEnvelope] = []

    async def handler(envelope: DurableEnvelope) -> None:
        received.append(envelope)
        durable_call_success()

    function = _make_link_activity(
        _LinkRegistration(
            LinkId(3, 4),
            "Automation Service",
            "Consume Durable Job",
            "Process Durable Job",
            "automation_service.durable.consume_durable_job."
            "process_durable_job.v1",
            handler,
        )
    )
    envelope = _envelope()
    await function(envelope)

    assert received == [envelope]
    with pytest.raises(ValueError, match="invalid durable envelope"):
        await function(_envelope(to_id=9))


@pytest.mark.asyncio
async def test_temporal_workflow_request_round_trips_through_sdk_converter() -> None:
    request = _DurableWorkflowRequest(
        activity_type=(
            "automation_service.durable.consume_durable_job."
            "process_durable_job.v1"
        ),
        continuation_activity_type=(
            "automation_service.durable_continuation.temporal.v1"
        ),
        activity_start_to_close_millis=1_000,
        activity_heartbeat_millis=0,
        maximum_attempts=3,
        priority=3,
        envelope=_envelope(),
    )
    converter = DataConverter.default
    payloads = await converter.encode([request])
    decoded = await converter.decode(payloads, [_DurableWorkflowRequest])

    assert decoded == [request]


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

    assert connector._endpoints[11].activity_type == "temporal.endpoint.durable_job.v1"
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
                "servicelib.callId": "call-1",
            }

    class _Handle:
        async def describe(self) -> _Description:
            return _Description()

    await _validate_workflow_ownership(
        _Handle(), "servicelib.test", "owner-1", "call-1"
    )

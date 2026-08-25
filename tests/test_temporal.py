from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from temporalio.converter import DataConverter

from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.runtime.common import DurableEnvelope
from pyservicelib_gorundebug.runtime.config import LinkId
from pyservicelib_gorundebug.datasource.temporal.connector import (
    Connector,
    _DurableWorkflowRequest,
    _LinkRegistration,
    _make_link_activity,
    _scheduled_time_nanos,
    _validate_workflow_ownership,
)


class _Config:
    def __init__(self) -> None:
        self.connector = SimpleNamespace(
            id=7,
            name="temporal",
            implementation=DataConnectorImplementation.TemporalPython,
        )
        self.endpoint = SimpleNamespace(id=11, id_data_connector=7)
        self.streams = [
            SimpleNamespace(
                id=31,
                id_service=2,
                id_endpoint=11,
                type=TransformationType.Input,
            )
        ]

    def get_data_connector_by_id(self, connector_id: int):
        assert connector_id == 7
        return self.connector

    def get_endpoint_config_by_id(self, endpoint_id: int):
        assert endpoint_id == 11
        return self.endpoint


class _Environment:
    def __init__(self) -> None:
        self.config = _Config()
        self.service_config = SimpleNamespace(id=1)


def _envelope(*, to_id: int = 4) -> DurableEnvelope:
    return DurableEnvelope(
        version=1,
        from_id=3,
        to_id=to_id,
        call_id="call-1",
        stream_id="stream-1",
        priority=0,
        deadline_unix_nano=0,
        sampling_enabled=False,
        payload=b"value",
        trace_carrier={
            "traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
        },
    )


def test_scheduled_time_uses_temporal_schedule_workflow_id_suffix() -> None:
    fallback = datetime(2026, 8, 24, 12, 35, 1, tzinfo=timezone.utc)
    assert _scheduled_time_nanos(
        "servicegen/schedule/1/3-2026-08-24T12:30:00.123456789Z",
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

    function = _make_link_activity(
        _LinkRegistration(LinkId(3, 4), "servicegen.test", handler)
    )
    envelope = _envelope()
    await function(envelope)

    assert received == [envelope]
    with pytest.raises(ValueError, match="invalid durable envelope"):
        await function(_envelope(to_id=9))


@pytest.mark.asyncio
async def test_temporal_workflow_request_round_trips_through_sdk_converter() -> None:
    request = _DurableWorkflowRequest(
        activity_type="servicegen.test",
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


def test_remote_endpoint_activity_identity_uses_input_service() -> None:
    connector = Connector(7, _Environment())  # type: ignore[arg-type]

    connector.register_endpoint_submission(11)

    assert connector._endpoints[11].activity_type == "servicegen.endpoint.2.11.v1"
    assert connector._endpoints[11].handler is None


@pytest.mark.asyncio
async def test_workflow_ownership_awaits_decoded_memo() -> None:
    class _Description:
        id = "workflow-1"
        workflow_type = "servicegen.test"

        async def memo(self) -> dict[str, str]:
            return {
                "servicegen.managedBy": "servicegen",
                "servicegen.owner": "owner-1",
                "servicegen.callId": "call-1",
            }

    class _Handle:
        async def describe(self) -> _Description:
            return _Description()

    await _validate_workflow_ownership(
        _Handle(), "servicegen.test", "owner-1", "call-1"
    )

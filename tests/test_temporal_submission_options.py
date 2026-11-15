import json
from dataclasses import asdict
from datetime import timedelta
from types import SimpleNamespace

import pytest

from pyservicelib_gorundebug.api.models.temporal_execution_type import TemporalExecutionType
from pyservicelib_gorundebug.datasource.temporal.submission_options import SubmissionOptions
from pyservicelib_gorundebug.datasource.temporal.workflow import (
    EndpointEnvelope,
    EndpointResult,
    EndpointWorkflowRequest,
    WorkflowEndpointConfig,
    WorkflowSubmission,
    submit_endpoint_from_workflow,
    WORKFLOW_SUBMISSION,
)
from pyservicelib_gorundebug.datasource.temporal.workflow_sink import make_workflow_sink_endpoint_consumer
from pyservicelib_gorundebug.runtime.context import request_stream_id


def test_submission_options_defaults_and_validation():
    assert SubmissionOptions().resolve("configured", TemporalExecutionType.ACTIVITY) == "configured"
    assert SubmissionOptions("selected").resolve("configured", TemporalExecutionType.WORKFLOW) == "selected"
    for options, execution in (
        (SubmissionOptions("  "), TemporalExecutionType.ACTIVITY),
        (SubmissionOptions("queue\0"), TemporalExecutionType.ACTIVITY),
        (SubmissionOptions(schedule_to_start_timeout=timedelta(seconds=-1)), TemporalExecutionType.ACTIVITY),
        (SubmissionOptions(schedule_to_start_timeout=timedelta(seconds=1)), TemporalExecutionType.WORKFLOW),
    ):
        with pytest.raises(ValueError):
            options.resolve("configured", execution)


@pytest.mark.asyncio
async def test_workflow_options_reach_actual_activity_call_without_mutation(monkeypatch):
    calls = []

    async def execute_activity(name, envelope, **options):
        calls.append(options)
        return EndpointResult(b"ok")

    monkeypatch.setattr(
        "pyservicelib_gorundebug.datasource.temporal.workflow.workflow.execute_activity",
        execute_activity,
    )
    cfg = WorkflowEndpointConfig(1, "operation", "configured", TemporalExecutionType.ACTIVITY,
                                 "operation-activity", "", 10000, 1000, 0, 1)
    submission = WorkflowSubmission("temporal", {1: cfg})
    envelope = EndpointEnvelope(1, 1, "call", "stream", 0)
    options = SubmissionOptions("selected", timedelta(seconds=2))
    await submit_endpoint_from_workflow(submission, 1, envelope, options)
    await submit_endpoint_from_workflow(submission, 1, envelope)
    assert calls[0]["task_queue"] == "selected"
    assert calls[0]["schedule_to_start_timeout"] == timedelta(seconds=2)
    assert calls[1]["task_queue"] == "configured"
    assert calls[1]["schedule_to_start_timeout"] is None
    assert submission.endpoints[1].task_queue == "configured"


def test_new_wrapper_timeout_is_json_serializable():
    request = EndpointWorkflowRequest("activity", 1000, 0, 1, 0,
                                      EndpointEnvelope(1, 1, "id", "stream", 0), 1234)
    data = asdict(request)
    # Payload bytes have the existing Temporal JSON converter; scheduling data
    # itself must not introduce an unsupported timedelta into the wire record.
    data.pop("envelope")
    assert json.loads(json.dumps(data))["activity_schedule_to_start_micros"] == 1234


@pytest.mark.asyncio
async def test_workflow_sink_factory_passes_typed_options_to_activity(monkeypatch):
    calls = []

    async def execute_activity(name, envelope, **options):
        calls.append((envelope, options))
        return EndpointResult(b"ok")

    monkeypatch.setattr(
        "pyservicelib_gorundebug.datasource.temporal.workflow.workflow.execute_activity",
        execute_activity,
    )
    cfg = WorkflowEndpointConfig(1, "operation", "configured", TemporalExecutionType.ACTIVITY,
                                 "operation-activity", "", 10000, 1000, 0, 1)
    bound = []
    stream = SimpleNamespace(name="route", endpoint_id=1,
                             serde=SimpleNamespace(serialize=lambda value: value.encode()),
                             set_sink_consumer=bound.append)
    consumer = make_workflow_sink_endpoint_consumer(
        stream, options=lambda value: SubmissionOptions(value, timedelta(seconds=2))
    )
    submission = WORKFLOW_SUBMISSION.set(WorkflowSubmission("temporal", {1: cfg}))
    identity = request_stream_id.set("parent")
    try:
        await consumer.consume("selected")
    finally:
        request_stream_id.reset(identity)
        WORKFLOW_SUBMISSION.reset(submission)
    assert bound == [consumer]
    assert calls[0][0].payload == b"selected"
    assert calls[0][1]["task_queue"] == "selected"
    assert calls[0][1]["schedule_to_start_timeout"] == timedelta(seconds=2)


@pytest.mark.asyncio
async def test_explicit_queue_rejects_existing_workflow_on_another_queue():
    from pyservicelib_gorundebug.datasource.temporal.connector import _validate_workflow_ownership

    async def memo():
        return {"servicelib.managedBy": "servicelib", "servicelib.owner": "owner", "servicelib.callId": "call"}

    async def describe():
        return SimpleNamespace(workflow_type="operation", id="existing", task_queue="previous", memo=memo)

    handle = SimpleNamespace(describe=describe)
    await _validate_workflow_ownership(handle, "operation", "owner", "call")
    with pytest.raises(RuntimeError, match="task queue collision"):
        await _validate_workflow_ownership(handle, "operation", "owner", "call", "selected")

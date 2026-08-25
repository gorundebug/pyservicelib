#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Workflow-isolate-safe Temporal endpoint contracts and execution.

This module intentionally imports no Temporal client/worker, process runtime,
filesystem, sockets, threads, or ServiceApp implementation. Generated static
Workflow classes may import it under the default Temporal sandbox.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, cast
from urllib.parse import quote

from temporalio import workflow
from temporalio.common import (
    Priority,
    RetryPolicy,
    WorkflowIDReusePolicy,
)

from ...api.models.temporal_execution_type import TemporalExecutionType
from ...runtime.durable_context import (
    DurableCallContext,
    TemporalContinueAsNewRequest,
    run_durable_call_workflow,
)
from ...runtime.common import Consumer, TypedInputStream
from ...runtime.context import (
    Context,
    request_cancelled,
    request_deadline,
    request_priority,
    request_stream_id,
)
from ...runtime.schedule import normalize_temporal_priority


ENDPOINT_WORKFLOW_TYPE = "servicelib.temporal-endpoint.v1"
_SCHEDULE_WORKFLOW_ID_SUFFIX = re.compile(
    r"-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$"
)


def _opaque_identity_component(value: str) -> str:
    return quote(value, safe="-._~")


# Intentionally identical to servicegen.splitWords + ToSnakeCase.
def _identity_name(value: str) -> str:
    words: list[str] = []
    current: list[str] = []
    characters = list(value)
    for index, character in enumerate(characters):
        if character.isspace() or character in "_-/.":
            if current:
                words.append("".join(current))
                current.clear()
            continue
        if not character.isalpha() and not character.isdigit():
            continue
        if current and character.isupper():
            previous = current[-1]
            if not previous.isupper() or (
                index + 1 < len(characters) and characters[index + 1].islower()
            ):
                words.append("".join(current))
                current.clear()
        current.append(character)
    if current:
        words.append("".join(current))
    return "_".join(word.lower() for word in words)


def _endpoint_workflow_id(
    connector_name: str, endpoint_name: str, message_id: str
) -> str:
    return (
        f"{_identity_name(connector_name)}/endpoint/"
        f"{_identity_name(endpoint_name)}/"
        f"{_opaque_identity_component(message_id)}"
    )


def _direct_workflow_type(connector_name: str, endpoint_name: str) -> str:
    return (
        f"{_identity_name(connector_name)}.endpoint."
        f"{_identity_name(endpoint_name)}.workflow.v1"
    )


def _schedule_workflow_id(connector_name: str, endpoint_name: str) -> str:
    return (
        f"{_identity_name(connector_name)}/schedule/"
        f"{_identity_name(endpoint_name)}"
    )


@dataclass(frozen=True, slots=True)
class EndpointEnvelope:
    version: int
    endpoint_id: int
    message_id: str
    stream_id: str
    priority: int
    deadline_unix_nano: int = 0
    scheduled: bool = False
    schedule_id: str = ""
    scheduled_at_unix_nano: int = 0
    fired_at_unix_nano: int = 0
    payload: bytes = b""


@dataclass(frozen=True, slots=True)
class EndpointResult:
    payload: bytes = b""


EndpointHandler = Callable[[EndpointEnvelope], Awaitable[EndpointResult]]
EndpointEncoder = Callable[[Any], bytes]


class WorkflowGraphEnvironment(Protocol):
    async def start(self, ctx: Context) -> None: ...

    async def finish(self) -> None: ...


class _WorkflowResultConsumer(Consumer[Any]):
    def __init__(self, future: asyncio.Future[Any]) -> None:
        self._future = future

    async def consume(self, value: Any) -> None:
        if self._future.done():
            raise RuntimeError("Temporal Workflow endpoint produced duplicate result")
        self._future.set_result(value)


@dataclass(frozen=True, slots=True)
class EndpointWorkflowRequest:
    activity_type: str
    activity_start_to_close_millis: int
    activity_heartbeat_millis: int
    maximum_attempts: int
    priority: int
    envelope: EndpointEnvelope


@dataclass(frozen=True, slots=True)
class WorkflowEndpointConfig:
    endpoint_id: int
    name: str
    task_queue: str
    execution_type: TemporalExecutionType
    activity_type: str
    workflow_type: str
    workflow_execution_millis: int
    activity_start_to_close_millis: int
    activity_heartbeat_millis: int
    maximum_attempts: int


@dataclass(frozen=True, slots=True)
class DirectEndpointWorkflowRequest:
    connector_name: str
    envelope: EndpointEnvelope
    endpoints: tuple[WorkflowEndpointConfig, ...]
    runtime_config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkflowSubmission:
    connector_name: str
    endpoints: dict[int, WorkflowEndpointConfig]
    runtime_config: dict[str, Any] = field(default_factory=dict)


WORKFLOW_SUBMISSION: ContextVar[WorkflowSubmission | None] = ContextVar(
    "servicelib_temporal_workflow_submission", default=None
)


def _endpoint_envelope(value: EndpointEnvelope | dict[str, Any]) -> EndpointEnvelope:
    if isinstance(value, EndpointEnvelope):
        return value
    data = dict(value)
    payload = data.get("payload", b"")
    if not isinstance(payload, bytes):
        data["payload"] = bytes(payload)
    return EndpointEnvelope(**data)


def _workflow_endpoint_config(
    value: WorkflowEndpointConfig | dict[str, Any],
) -> WorkflowEndpointConfig:
    if isinstance(value, WorkflowEndpointConfig):
        return value
    data = dict(value)
    execution_type = data.get("execution_type")
    if not isinstance(execution_type, TemporalExecutionType):
        data["execution_type"] = TemporalExecutionType(execution_type)
    return WorkflowEndpointConfig(**data)


def direct_workflow_request(
    value: DirectEndpointWorkflowRequest | dict[str, Any],
) -> DirectEndpointWorkflowRequest:
    if isinstance(value, DirectEndpointWorkflowRequest):
        return value
    return DirectEndpointWorkflowRequest(
        connector_name=str(value["connector_name"]),
        envelope=_endpoint_envelope(value["envelope"]),
        endpoints=tuple(
            _workflow_endpoint_config(item) for item in value.get("endpoints", ())
        ),
        runtime_config=dict(value.get("runtime_config", {})),
    )


def _scheduled_time_nanos(workflow_id: str, fallback: datetime) -> int:
    match = _SCHEDULE_WORKFLOW_ID_SUFFIX.search(workflow_id)
    if match is not None:
        seconds = datetime.strptime(
            match.group(1), "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        fraction = (match.group(2) or "").ljust(9, "0")
        return int(seconds.timestamp()) * 1_000_000_000 + int(fraction or "0")
    return int(fallback.astimezone(timezone.utc).timestamp() * 1_000_000_000)


@workflow.defn(name=ENDPOINT_WORKFLOW_TYPE)
class TemporalEndpointWorkflow:
    """Infrastructure Workflow for an Activity endpoint."""

    @workflow.run
    async def run(self, request: EndpointWorkflowRequest) -> EndpointResult:
        envelope = request.envelope
        if envelope.scheduled:
            info = workflow.info()
            envelope = replace(
                envelope,
                message_id=info.workflow_id,
                stream_id=info.workflow_id,
                scheduled_at_unix_nano=_scheduled_time_nanos(
                    info.workflow_id, info.workflow_start_time
                ),
                fired_at_unix_nano=int(
                    workflow.now().astimezone(timezone.utc).timestamp()
                    * 1_000_000_000
                ),
            )
        return cast(
            EndpointResult,
            await workflow.execute_activity(
                request.activity_type,
                envelope,
                result_type=EndpointResult,
                start_to_close_timeout=timedelta(
                    milliseconds=request.activity_start_to_close_millis
                ),
                heartbeat_timeout=(
                    timedelta(milliseconds=request.activity_heartbeat_millis)
                    if request.activity_heartbeat_millis > 0
                    else None
                ),
                retry_policy=RetryPolicy(maximum_attempts=request.maximum_attempts),
                priority=Priority(priority_key=request.priority),
            ),
        )


async def submit_endpoint_from_workflow(
    submission: WorkflowSubmission,
    endpoint_id: int,
    envelope: EndpointEnvelope,
) -> EndpointResult:
    cfg = submission.endpoints.get(endpoint_id)
    if cfg is None:
        raise ValueError(
            f"Temporal endpoint {endpoint_id} is absent from Workflow "
            "configuration snapshot"
        )
    if not envelope.message_id or not envelope.stream_id:
        raise ValueError(
            f"Temporal Workflow submission to {cfg.name!r} requires stable identity"
        )
    envelope = replace(envelope, version=1, endpoint_id=endpoint_id)
    retry_policy = RetryPolicy(maximum_attempts=cfg.maximum_attempts)
    priority = Priority(
        priority_key=normalize_temporal_priority(envelope.priority)
    )
    if cfg.execution_type is TemporalExecutionType.ACTIVITY:
        return cast(
            EndpointResult,
            await workflow.execute_activity(
                cfg.activity_type,
                envelope,
                result_type=EndpointResult,
                task_queue=cfg.task_queue,
                start_to_close_timeout=timedelta(
                    milliseconds=cfg.activity_start_to_close_millis
                ),
                heartbeat_timeout=(
                    timedelta(milliseconds=cfg.activity_heartbeat_millis)
                    if cfg.activity_heartbeat_millis > 0
                    else None
                ),
                retry_policy=retry_policy,
                priority=priority,
            ),
        )
    if cfg.execution_type is TemporalExecutionType.WORKFLOW:
        request = DirectEndpointWorkflowRequest(
            connector_name=submission.connector_name,
            envelope=envelope,
            endpoints=tuple(
                submission.endpoints[key] for key in sorted(submission.endpoints)
            ),
            runtime_config=submission.runtime_config,
        )
        return cast(
            EndpointResult,
            await workflow.execute_child_workflow(
                cfg.workflow_type,
                request,
                id=_endpoint_workflow_id(
                    submission.connector_name, cfg.name, envelope.message_id
                ),
                task_queue=cfg.task_queue,
                result_type=EndpointResult,
                execution_timeout=(
                    timedelta(milliseconds=cfg.workflow_execution_millis)
                    if cfg.workflow_execution_millis > 0
                    else None
                ),
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                retry_policy=retry_policy,
                priority=priority,
            ),
        )
    raise ValueError(
        f"Temporal endpoint {cfg.name!r} has unsupported execution type "
        f"{cfg.execution_type!r}"
    )


async def execute_direct_endpoint_workflow(
    request: DirectEndpointWorkflowRequest | dict[str, Any],
    *,
    endpoint_id: int,
    workflow_type: str,
    handler: EndpointHandler,
    encode_input: EndpointEncoder,
) -> EndpointResult:
    """Execute one statically generated direct-Workflow endpoint."""

    parsed = direct_workflow_request(request)
    envelope = parsed.envelope
    if envelope.version != 1 or envelope.endpoint_id != endpoint_id:
        raise ValueError(f"invalid Temporal Workflow envelope for {endpoint_id}")
    if envelope.scheduled:
        info = workflow.info()
        envelope = replace(
            envelope,
            message_id=info.workflow_id,
            stream_id=info.workflow_id,
            scheduled_at_unix_nano=_scheduled_time_nanos(
                info.workflow_id, info.workflow_start_time
            ),
            fired_at_unix_nano=int(
                workflow.now().astimezone(timezone.utc).timestamp()
                * 1_000_000_000
            ),
        )
    if not envelope.message_id or not envelope.stream_id:
        raise ValueError(f"invalid Temporal Workflow identity for {endpoint_id}")
    durable = DurableCallContext(
        envelope.message_id,
        delay=workflow.sleep,
        workflow=True,
        recording_policy=lambda: not workflow.unsafe.is_replaying(),
    )
    submission_token = WORKFLOW_SUBMISSION.set(
        WorkflowSubmission(
            connector_name=parsed.connector_name,
            endpoints={item.endpoint_id: item for item in parsed.endpoints},
            runtime_config=parsed.runtime_config,
        )
    )
    try:
        try:
            return await run_durable_call_workflow(
                durable, lambda: handler(envelope)
            )
        finally:
            WORKFLOW_SUBMISSION.reset(submission_token)
    except TemporalContinueAsNewRequest as continuation:
        next_envelope = replace(
            envelope,
            scheduled=False,
            schedule_id="",
            scheduled_at_unix_nano=0,
            fired_at_unix_nano=0,
            payload=encode_input(continuation.next_input),
        )
        workflow.continue_as_new(
            replace(parsed, envelope=next_envelope),
            workflow=cast(Any, workflow_type),
        )


async def execute_workflow_graph_endpoint(
    *,
    environment: WorkflowGraphEnvironment,
    stream: TypedInputStream[Any, Any, Any],
    envelope: EndpointEnvelope,
    activate: Callable[[EndpointEnvelope], Awaitable[None]],
) -> EndpointResult:
    """Run one generated graph activation inside the Workflow isolate."""

    result_stream = stream.get_result_stream()
    result: asyncio.Future[Any] | None = None
    if result_stream is not None:
        result = asyncio.get_running_loop().create_future()
        stream.set_result_consumer(_WorkflowResultConsumer(result))

    deadline = (
        datetime.fromtimestamp(
            envelope.deadline_unix_nano / 1_000_000_000,
            tz=timezone.utc,
        )
        if envelope.deadline_unix_nano > 0
        else None
    )
    stream_token = request_stream_id.set(envelope.stream_id)
    priority_token = request_priority.set(envelope.priority)
    deadline_token = request_deadline.set(deadline)
    cancelled_token = request_cancelled.set(asyncio.Event())
    await environment.start(Context())
    execution_error: BaseException | None = None
    try:
        await activate(envelope)
        if result is None:
            return EndpointResult()
        value = await result
        if result_stream is None:
            raise RuntimeError("Temporal Workflow result stream disappeared")
        return EndpointResult(bytes(result_stream.serde.serialize(value)))
    except BaseException as error:
        execution_error = error
        raise
    finally:
        try:
            await environment.finish()
        except BaseException as cleanup_error:
            if execution_error is None:
                raise
            raise BaseExceptionGroup(
                "Temporal Workflow execution and graph cleanup both failed",
                [execution_error, cleanup_error],
            ) from cleanup_error
        finally:
            request_cancelled.reset(cancelled_token)
            request_deadline.reset(deadline_token)
            request_priority.reset(priority_token)
            request_stream_id.reset(stream_token)


# Private aliases remain temporarily for the existing internal test surface.
_EndpointWorkflowRequest = EndpointWorkflowRequest
_WorkflowEndpointConfig = WorkflowEndpointConfig
_DirectEndpointWorkflowRequest = DirectEndpointWorkflowRequest
_WorkflowSubmission = WorkflowSubmission
_WORKFLOW_SUBMISSION = WORKFLOW_SUBMISSION
_direct_workflow_request = direct_workflow_request
_submit_endpoint_from_workflow = submit_endpoint_from_workflow
_TemporalEndpointWorkflow = TemporalEndpointWorkflow

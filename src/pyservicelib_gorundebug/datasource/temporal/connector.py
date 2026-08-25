#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Temporal transport boundary for symmetric source/sink endpoints.

The Workflows and Activities in this module are infrastructure only. An
Activity activates an already existing graph consumer; it never replaces or
changes the target node's business function.
"""

from __future__ import annotations

import asyncio
import os
import re
import threading
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, cast
from urllib.parse import quote

from temporalio import activity, workflow
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)
from temporalio.common import (
    Priority,
    RetryPolicy,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.service import TLSConfig
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from ...api.models.data_connector_implementation import DataConnectorImplementation
from ...api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from ...api.models.schedule_overlap_policy import ScheduleOverlapPolicy as ApiOverlapPolicy
from ...api.models.temporal_execution_type import TemporalExecutionType
from ...runtime.common import ManagedDataConnector, ServiceExecutionEnvironment
from ...runtime.config import EndpointConfig
from ...runtime.context import Context
from ...runtime.durable_context import (
    DurableCallContext,
    DurableCallDiagnostics,
    TemporalContinueAsNewRequest,
    run_durable_call_activity,
    run_durable_call_workflow,
)
from ...runtime.environment.log import err_field, str_field
from ...runtime.schedule import normalize_temporal_priority
from .context_propagation import TemporalContextPropagationInterceptor


ENDPOINT_WORKFLOW_TYPE = "servicelib.temporal-endpoint.v1"
_MEMO_MANAGED_BY = "servicelib.managedBy"
_MEMO_OWNER = "servicelib.owner"
_MEMO_CALL_ID = "servicelib.callId"
_SCHEDULE_WORKFLOW_ID_SUFFIX = re.compile(
    r"-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$"
)
_SDK_METRICS_BIND_ADDRESS_ENVIRONMENT = "TEMPORAL_SDK_METRICS_BIND_ADDRESS"
_SDK_RUNTIME_LOCK = threading.Lock()
_SDK_RUNTIME: Runtime | None = None
_SDK_RUNTIME_ADDRESS: str | None = None


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


def _endpoint_owner(connector_name: str, endpoint_name: str) -> str:
    return (
        f"{_identity_name(connector_name)}/endpoint/"
        f"{_identity_name(endpoint_name)}/v1"
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


def _sdk_runtime() -> Runtime | None:
    """Return one process-wide Temporal runtime with official SDK metrics."""

    address = os.environ.get(_SDK_METRICS_BIND_ADDRESS_ENVIRONMENT, "").strip()
    if not address:
        return None
    global _SDK_RUNTIME, _SDK_RUNTIME_ADDRESS
    with _SDK_RUNTIME_LOCK:
        if _SDK_RUNTIME is not None:
            if _SDK_RUNTIME_ADDRESS != address:
                raise RuntimeError(
                    "Temporal SDK metrics already listen on "
                    f"{_SDK_RUNTIME_ADDRESS!r}, cannot also use {address!r}"
                )
            return _SDK_RUNTIME
        _SDK_RUNTIME = Runtime(
            telemetry=TelemetryConfig(
                metrics=PrometheusConfig(
                    bind_address=address,
                    durations_as_seconds=True,
                )
            )
        )
        _SDK_RUNTIME_ADDRESS = address
        return _SDK_RUNTIME


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


@dataclass(frozen=True, slots=True)
class _EndpointWorkflowRequest:
    activity_type: str
    activity_start_to_close_millis: int
    activity_heartbeat_millis: int
    maximum_attempts: int
    priority: int
    envelope: EndpointEnvelope


@dataclass(frozen=True, slots=True)
class _WorkflowEndpointConfig:
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
class _DirectEndpointWorkflowRequest:
    connector_name: str
    envelope: EndpointEnvelope
    endpoints: tuple[_WorkflowEndpointConfig, ...]


@dataclass(frozen=True, slots=True)
class _WorkflowSubmission:
    connector_name: str
    endpoints: dict[int, _WorkflowEndpointConfig]


_WORKFLOW_SUBMISSION: ContextVar[_WorkflowSubmission | None] = ContextVar(
    "servicelib_temporal_workflow_submission", default=None
)


@workflow.defn(name=ENDPOINT_WORKFLOW_TYPE, sandboxed=False)
class _TemporalEndpointWorkflow:
    @workflow.run
    async def run(self, request: _EndpointWorkflowRequest) -> EndpointResult:
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
            )
        result = await workflow.execute_activity(
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
        )
        return result


def _scheduled_time_nanos(workflow_id: str, fallback: datetime) -> int:
    match = _SCHEDULE_WORKFLOW_ID_SUFFIX.search(workflow_id)
    if match is not None:
        seconds = datetime.strptime(
            match.group(1), "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        fraction = (match.group(2) or "").ljust(9, "0")
        return int(seconds.timestamp()) * 1_000_000_000 + int(fraction or "0")
    return int(fallback.astimezone(timezone.utc).timestamp() * 1_000_000_000)


@dataclass(frozen=True, slots=True)
class _EndpointRegistration:
    endpoint_id: int
    activity_type: str
    handler: Optional[EndpointHandler]
    workflow_type: str = ""
    encode_input: EndpointEncoder | None = None


@dataclass(slots=True)
class _QueueRegistration:
    activities: list[Callable[..., Awaitable[Any]]]
    workflows: list[type[Any]]
    endpoint_workflow: bool = False


def _required_string(obj: Any, name: str) -> str:
    value = getattr(obj, name, None)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{type(obj).__name__}.{name} must be a non-empty string")
    return value


def _temporal_cron_expression(expression: str) -> str:
    """Translate the portable five-field DSL cron into Temporal's format."""
    return "0 " + " ".join(expression.split())


def _integer(obj: Any, name: str, default: int = 0) -> int:
    value = getattr(obj, name, None)
    return value if isinstance(value, int) else default


def _bool(obj: Any, name: str, default: bool = False) -> bool:
    value = getattr(obj, name, None)
    return value if isinstance(value, bool) else default


class Connector(ManagedDataConnector):
    """One official Temporal client and its generated Worker registrations."""

    def __init__(self, connector_id: int, environment: ServiceExecutionEnvironment):
        cfg = environment.config.get_data_connector_by_id(connector_id)
        self._id = connector_id
        self._name = cfg.name
        self._environment = environment
        self._endpoints: dict[int, _EndpointRegistration] = {}
        self._client: Optional[Client] = None
        self._workers: list[Worker] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._activity_events = environment.metrics.scope(
            "temporal_activity", {"connector": self._name}
        ).counter_vec(
            "events_total", "Total number of Temporal Activity lifecycle events"
        )
        self._started = False

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    def _config(self) -> Any:
        return self._environment.config.get_data_connector_by_id(self._id)

    def _durable_diagnostics(
        self, boundary: str, target: str
    ) -> DurableCallDiagnostics:
        def report(event: str, error: BaseException | None) -> None:
            self._activity_events.with_({
                "boundary": boundary,
                "target": target,
                "event": event,
            }).inc()
            if error is None:
                return
            fields = (
                str_field("connector", self._name),
                str_field("boundary", boundary),
                str_field("target", target),
                str_field("event", event),
                err_field(error),
            )
            if event == "late_heartbeat":
                self._environment.log.warn(
                    "Temporal Activity lifecycle misuse", *fields
                )
            else:
                self._environment.log.error("Temporal Activity failed", *fields)

        return report

    def register_endpoint(
        self,
        endpoint_id: int,
        handler: EndpointHandler,
        encode_input: EndpointEncoder,
    ) -> None:
        if self._started:
            raise RuntimeError("cannot register endpoint after Temporal connector start")
        existing = self._endpoints.get(endpoint_id)
        if existing is not None and existing.handler is not None:
            raise ValueError(f"Temporal endpoint {endpoint_id} is already registered")
        cfg = self._environment.config.get_endpoint_config_by_id(endpoint_id)
        if cfg.id_data_connector != self._id:
            raise ValueError(
                f"endpoint {endpoint_id} does not belong to Temporal connector {self._name!r}"
            )
        registration = existing or self._endpoint_registration(endpoint_id)
        self._endpoints[endpoint_id] = replace(
            registration, handler=handler, encode_input=encode_input
        )

    def register_endpoint_submission(self, endpoint_id: int) -> None:
        """Register only the immutable remote endpoint contract for a sink."""

        if endpoint_id not in self._endpoints:
            self._endpoints[endpoint_id] = self._endpoint_registration(endpoint_id)

    def _endpoint_registration(self, endpoint_id: int) -> _EndpointRegistration:
        cfg = self._environment.config.get_endpoint_config_by_id(endpoint_id)
        if cfg.id_data_connector != self._id:
            raise ValueError(
                f"endpoint {endpoint_id} does not belong to Temporal connector {self._name!r}"
            )
        return _EndpointRegistration(
            endpoint_id=endpoint_id,
            activity_type=(
                f"{_identity_name(self._name)}.endpoint."
                f"{_identity_name(cfg.name)}.v1"
            ),
            handler=None,
            workflow_type=_direct_workflow_type(self._name, cfg.name),
        )

    def _workflow_endpoint_snapshot(self) -> tuple[_WorkflowEndpointConfig, ...]:
        result: list[_WorkflowEndpointConfig] = []
        for endpoint_id in sorted(self._endpoints):
            registration = self._endpoints[endpoint_id]
            cfg = self._endpoint_config(endpoint_id)
            result.append(
                _WorkflowEndpointConfig(
                    endpoint_id=endpoint_id,
                    name=cfg.name,
                    task_queue=_required_string(cfg, "task_queue"),
                    execution_type=cast(
                        TemporalExecutionType, cfg.temporal_execution_type
                    ),
                    activity_type=registration.activity_type,
                    workflow_type=registration.workflow_type,
                    workflow_execution_millis=_integer(
                        cfg, "workflow_execution_timeout"
                    ),
                    activity_start_to_close_millis=_integer(
                        cfg, "activity_start_to_close_timeout"
                    ),
                    activity_heartbeat_millis=_integer(
                        cfg, "activity_heartbeat_timeout"
                    ),
                    maximum_attempts=_integer(cfg, "maximum_attempts"),
                )
            )
        return tuple(result)

    async def start(self, ctx: Context) -> None:
        if self._started:
            return
        cfg = self._config()
        tls = self._tls_config(cfg)
        context_interceptor = TemporalContextPropagationInterceptor(
            self._environment.tracing
        )
        connect = Client.connect(
            _required_string(cfg, "address"),
            namespace=_required_string(cfg, "namespace"),
            identity=getattr(cfg, "identity", None) or None,
            api_key=getattr(cfg, "api_key", None) or None,
            tls=tls,
            runtime=_sdk_runtime(),
            interceptors=[context_interceptor],
        )
        self._client = await (
            asyncio.wait_for(connect, timeout=ctx.time_left)
            if ctx.time_left is not None
            else connect
        )
        # A Worker may receive an existing backlog as soon as its polling task
        # starts.  Outbound submissions from that Activity must therefore be
        # admitted before Worker polling begins, not after schedule
        # reconciliation has completed.
        self._started = True
        queues = self._build_queue_registrations()
        try:
            for task_queue, registration in queues.items():
                workflows = list(registration.workflows)
                if registration.endpoint_workflow:
                    workflows.append(_TemporalEndpointWorkflow)
                worker = Worker(
                    self._client,
                    task_queue=task_queue,
                    activities=registration.activities,
                    workflows=workflows,
                    max_concurrent_activities=(
                        _integer(cfg, "max_concurrent_activities") or None
                    ),
                    max_concurrent_workflow_tasks=(
                        _integer(cfg, "max_concurrent_workflows") or None
                    ),
                    interceptors=[context_interceptor],
                )
                self._workers.append(worker)
                self._worker_tasks.append(asyncio.create_task(worker.run()))
            await asyncio.sleep(0)
            for task in self._worker_tasks:
                if task.done():
                    task.result()
            for endpoint_id in self._endpoints:
                endpoint_cfg = self._endpoint_config(endpoint_id)
                if _bool(endpoint_cfg, "enabled") and getattr(
                    endpoint_cfg, "schedule", None
                ):
                    await self._ensure_schedule(endpoint_cfg)
        except BaseException:
            self._started = False
            await self._shutdown_workers()
            self._client = None
            raise

    async def stop(self, ctx: Context) -> None:
        del ctx
        if not self._started and not self._workers:
            return
        self._started = False
        await self._shutdown_workers()
        self._client = None

    async def stop_admission(self, ctx: Context) -> None:
        """Stop Task Queue polling while preserving outbound submissions."""

        del ctx
        await self._shutdown_workers()

    async def _shutdown_workers(self) -> None:
        await asyncio.gather(
            *(worker.shutdown() for worker in self._workers),
            return_exceptions=True,
        )
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._workers.clear()
        self._worker_tasks.clear()

    def _build_queue_registrations(self) -> dict[str, _QueueRegistration]:
        queues: dict[str, _QueueRegistration] = {}
        for endpoint_id, endpoint_registration in self._endpoints.items():
            if endpoint_registration.handler is None:
                continue
            cfg = self._endpoint_config(endpoint_id)
            if not _bool(cfg, "enabled"):
                continue
            queue = queues.setdefault(
                _required_string(cfg, "task_queue"), _QueueRegistration([], [])
            )

            if cfg.temporal_execution_type is TemporalExecutionType.ACTIVITY:
                queue.activities.append(
                    _make_endpoint_activity(
                        endpoint_registration,
                        self._durable_diagnostics("endpoint", str(endpoint_id)),
                    )
                )
                queue.endpoint_workflow = True
            elif cfg.temporal_execution_type is TemporalExecutionType.WORKFLOW:
                queue.workflows.append(_make_endpoint_workflow(endpoint_registration))
            else:
                raise ValueError(
                    f"Temporal endpoint {cfg.name!r} has unsupported execution type"
                )
        return queues

    async def submit_endpoint(
        self,
        endpoint_id: int,
        envelope: EndpointEnvelope,
        wait_for_result: bool,
    ) -> EndpointResult:
        workflow_submission = _WORKFLOW_SUBMISSION.get()
        if workflow_submission is not None:
            return await _submit_endpoint_from_workflow(
                workflow_submission, endpoint_id, envelope
            )
        registration = self._endpoints.get(endpoint_id)
        if not self._started:
            raise RuntimeError(f"Temporal connector {self._name!r} is not started")
        client = self._require_client()
        if registration is None:
            raise ValueError(f"Temporal endpoint {endpoint_id} is not registered")
        cfg = self._endpoint_config(endpoint_id)
        if not _bool(cfg, "enabled"):
            raise RuntimeError(f"Temporal endpoint {cfg.name!r} is disabled")
        request = _EndpointWorkflowRequest(
            registration.activity_type,
            _integer(cfg, "activity_start_to_close_timeout"),
            _integer(cfg, "activity_heartbeat_timeout"),
            _integer(cfg, "maximum_attempts"),
            normalize_temporal_priority(envelope.priority),
            envelope,
        )
        workflow_type = ENDPOINT_WORKFLOW_TYPE
        workflow_input: _EndpointWorkflowRequest | _DirectEndpointWorkflowRequest = (
            request
        )
        if cfg.temporal_execution_type is TemporalExecutionType.WORKFLOW:
            workflow_type = registration.workflow_type
            workflow_input = _DirectEndpointWorkflowRequest(
                connector_name=self._name,
                envelope=envelope,
                endpoints=self._workflow_endpoint_snapshot(),
            )
        workflow_id = _endpoint_workflow_id(
            self._name, cfg.name, envelope.message_id
        )
        owner = _endpoint_owner(self._name, cfg.name)
        handle = await client.start_workflow(
            workflow_type,
            workflow_input,
            id=workflow_id,
            task_queue=_required_string(cfg, "task_queue"),
            execution_timeout=_optional_timeout(cfg, "workflow_execution_timeout"),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            memo=_ownership_memo(owner, envelope.message_id),
            priority=Priority(priority_key=request.priority),
        )
        await _validate_workflow_ownership(
            handle, workflow_type, owner, envelope.message_id
        )
        if not wait_for_result:
            return EndpointResult()
        return cast(EndpointResult, await handle.result())

    async def _ensure_schedule(self, cfg: EndpointConfig) -> None:
        client = self._require_client()
        endpoint_id = cfg.id
        registration = self._endpoints[endpoint_id]
        schedule_id = _required_string(cfg, "schedule_id")
        owner = _endpoint_owner(self._name, cfg.name)
        envelope = EndpointEnvelope(
            version=1,
            endpoint_id=endpoint_id,
            message_id="",
            stream_id="",
            priority=0,
            scheduled=True,
            schedule_id=schedule_id,
        )
        request = _EndpointWorkflowRequest(
            registration.activity_type,
            _integer(cfg, "activity_start_to_close_timeout"),
            _integer(cfg, "activity_heartbeat_timeout"),
            _integer(cfg, "maximum_attempts"),
            normalize_temporal_priority(0),
            envelope,
        )
        workflow_type = ENDPOINT_WORKFLOW_TYPE
        workflow_input: _EndpointWorkflowRequest | _DirectEndpointWorkflowRequest = (
            request
        )
        if cfg.temporal_execution_type is TemporalExecutionType.WORKFLOW:
            workflow_type = registration.workflow_type
            workflow_input = _DirectEndpointWorkflowRequest(
                connector_name=self._name,
                envelope=envelope,
                endpoints=self._workflow_endpoint_snapshot(),
            )
        overlap = ScheduleOverlapPolicy.ALLOW_ALL
        if getattr(cfg, "overlap_policy", None) == ApiOverlapPolicy.SKIP:
            overlap = ScheduleOverlapPolicy.SKIP
        catchup = timedelta(seconds=10)
        if getattr(cfg, "missed_run_policy", None) == ScheduleMissedRunPolicy.FIREONCE:
            catchup = timedelta(days=365)
        action = ScheduleActionStartWorkflow(
            workflow_type,
            workflow_input,
            id=_schedule_workflow_id(self._name, cfg.name),
            task_queue=_required_string(cfg, "task_queue"),
            execution_timeout=_optional_timeout(cfg, "workflow_execution_timeout"),
            memo=_ownership_memo(owner, schedule_id),
            priority=Priority(priority_key=request.priority),
        )
        try:
            await client.create_schedule(
                schedule_id,
                Schedule(
                    action,
                    ScheduleSpec(
                        cron_expressions=[_temporal_cron_expression(
                            _required_string(cfg, "schedule")
                        )],
                        time_zone_name=getattr(cfg, "timezone", None) or None,
                    ),
                    SchedulePolicy(overlap=overlap, catchup_window=catchup),
                ),
                memo=_ownership_memo(owner, schedule_id),
            )
        except ScheduleAlreadyRunningError:
            description = await client.get_schedule_handle(schedule_id).describe()
            _validate_memo(await description.memo(), owner, schedule_id)
            existing = description.schedule.action
            if (
                not isinstance(existing, ScheduleActionStartWorkflow)
                or existing.workflow != workflow_type
                or existing.task_queue != _required_string(cfg, "task_queue")
            ):
                raise RuntimeError(
                    f"Temporal schedule {schedule_id!r} ownership collision"
                ) from None

    def _endpoint_config(self, endpoint_id: int) -> EndpointConfig:
        return self._environment.config.get_endpoint_config_by_id(endpoint_id)

    def _require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError(f"Temporal connector {self._name!r} has no client")
        return self._client

    def _tls_config(self, cfg: Any) -> bool | TLSConfig | None:
        enabled = _bool(cfg, "tls_enabled")
        server_name = getattr(cfg, "tls_server_name", None) or None
        ca_file = getattr(cfg, "tls_ca_file", None) or None
        cert_file = getattr(cfg, "tls_cert_file", None) or None
        key_file = getattr(cfg, "tls_key_file", None) or None
        if not any((enabled, server_name, ca_file, cert_file, key_file)):
            return None
        if bool(cert_file) != bool(key_file):
            raise ValueError(
                f"Temporal connector {self._name!r} requires both TLS cert and key"
            )
        return TLSConfig(
            server_root_ca_cert=Path(ca_file).read_bytes() if ca_file else None,
            client_cert=Path(cert_file).read_bytes() if cert_file else None,
            client_private_key=Path(key_file).read_bytes() if key_file else None,
            domain=server_name,
            verification_server_name=server_name,
        )


def workflow_time_nanos() -> int:
    """Return Activity wall time; kept separate for deterministic unit tests."""
    from datetime import datetime

    return int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)


async def _submit_endpoint_from_workflow(
    submission: _WorkflowSubmission,
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
        request = _DirectEndpointWorkflowRequest(
            connector_name=submission.connector_name,
            envelope=envelope,
            endpoints=tuple(
                submission.endpoints[key]
                for key in sorted(submission.endpoints)
            ),
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


def _make_endpoint_activity(
    registration: _EndpointRegistration,
    diagnostics: DurableCallDiagnostics | None = None,
) -> Callable[[EndpointEnvelope], Awaitable[EndpointResult]]:
    async def invoke_endpoint(envelope: EndpointEnvelope) -> EndpointResult:
        if (
            envelope.version != 1
            or envelope.endpoint_id != registration.endpoint_id
            or not envelope.message_id
        ):
            raise ValueError(
                f"invalid Temporal endpoint envelope for "
                f"{registration.endpoint_id}"
            )
        handler = registration.handler
        if handler is None:
            raise RuntimeError(
                f"Temporal endpoint {registration.endpoint_id} has no local Activity handler"
            )
        fired = replace(envelope, fired_at_unix_nano=workflow_time_nanos())
        durable = DurableCallContext(
            fired.message_id,
            heartbeat=activity.heartbeat,
            diagnostics=diagnostics,
        )
        return await run_durable_call_activity(durable, lambda: handler(fired))

    return activity.defn(name=registration.activity_type)(invoke_endpoint)


def _make_endpoint_workflow(
    registration: _EndpointRegistration,
) -> type[Any]:
    class_name = (
        "TemporalEndpointWorkflow"
        f"{registration.endpoint_id}{_identity_name(registration.workflow_type)}"
    )

    async def run(
        _self: object, request: _DirectEndpointWorkflowRequest
    ) -> EndpointResult:
        envelope = request.envelope
        if envelope.version != 1 or envelope.endpoint_id != registration.endpoint_id:
            raise ValueError(
                f"invalid Temporal Workflow envelope for {registration.endpoint_id}"
            )
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
            raise ValueError(
                f"invalid Temporal Workflow identity for {registration.endpoint_id}"
            )
        handler = registration.handler
        if handler is None:
            raise RuntimeError(
                f"Temporal endpoint {registration.endpoint_id} has no "
                "local Workflow handler"
            )
        durable = DurableCallContext(
            envelope.message_id,
            delay=workflow.sleep,
            workflow=True,
            recording_policy=lambda: not workflow.unsafe.is_replaying(),
        )
        submission_token = _WORKFLOW_SUBMISSION.set(
            _WorkflowSubmission(
                connector_name=request.connector_name,
                endpoints={item.endpoint_id: item for item in request.endpoints},
            )
        )
        try:
            try:
                return await run_durable_call_workflow(
                    durable, lambda: handler(envelope)
                )
            finally:
                _WORKFLOW_SUBMISSION.reset(submission_token)
        except TemporalContinueAsNewRequest as continuation:
            if registration.encode_input is None:
                raise RuntimeError(
                    f"Temporal endpoint {registration.endpoint_id} has no input encoder"
                )
            next_envelope = replace(
                envelope,
                scheduled=False,
                schedule_id="",
                scheduled_at_unix_nano=0,
                fired_at_unix_nano=0,
                payload=registration.encode_input(continuation.next_input),
            )
            workflow.continue_as_new(
                replace(request, envelope=next_envelope),
                workflow=cast(Any, registration.workflow_type),
            )

    run.__name__ = "run"
    run.__qualname__ = f"{class_name}.run"
    workflow_class = type(
        class_name,
        (),
        {"__module__": __name__, "run": workflow.run(run)},
    )
    globals()[class_name] = workflow_class
    return workflow.defn(
        name=registration.workflow_type, sandboxed=False
    )(workflow_class)


def _optional_timeout(obj: Any, name: str) -> Optional[timedelta]:
    milliseconds = _integer(obj, name)
    return timedelta(milliseconds=milliseconds) if milliseconds > 0 else None


def _ownership_memo(owner: str, call_id: str) -> dict[str, str]:
    return {
        _MEMO_MANAGED_BY: "servicelib",
        _MEMO_OWNER: owner,
        _MEMO_CALL_ID: call_id,
    }


def _validate_memo(memo: Any, expected_owner: str, expected_call_id: str) -> None:
    expected = _ownership_memo(expected_owner, expected_call_id)
    for key, value in expected.items():
        if memo.get(key) != value:
            raise RuntimeError(
                f"Temporal ownership collision: memo {key}={memo.get(key)!r}, "
                f"expected {value!r}"
            )


async def _validate_workflow_ownership(
    handle: Any,
    expected_type: str,
    expected_owner: str,
    expected_call_id: str,
) -> None:
    description = await handle.describe()
    if description.workflow_type != expected_type:
        raise RuntimeError(
            f"Temporal workflow {description.id!r} ownership collision: "
            f"workflow type {description.workflow_type!r}, expected {expected_type!r}"
        )
    _validate_memo(await description.memo(), expected_owner, expected_call_id)


def make_connector(
    connector_id: int,
    environment: ServiceExecutionEnvironment,
) -> Connector:
    cfg = environment.config.get_data_connector_by_id(connector_id)
    implementation = cfg.implementation
    if implementation not in (
        DataConnectorImplementation.TemporalPython,
        DataConnectorImplementation.TemporalPython.value,
    ):
        raise ValueError(
            f"data connector id={connector_id} is not a temporal/python connector"
        )
    existing = environment.get_managed_data_connector(connector_id)
    if existing is not None:
        if not isinstance(existing, Connector):
            raise TypeError(
                f"managed connector id={connector_id} is not a Python Temporal connector"
            )
        return existing
    connector = Connector(connector_id, environment)
    environment.add_managed_data_connector(connector)
    return connector

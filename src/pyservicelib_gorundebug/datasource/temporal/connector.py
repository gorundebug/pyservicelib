#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Temporal transport boundary for DurableCall and symmetric endpoints.

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
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, cast

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
from ...api.models.transformation_type import TransformationType
from ...runtime.common import DurableEnvelope, DurableTransport, ServiceExecutionEnvironment
from ...runtime.config import EndpointConfig, LinkId
from ...runtime.context import Context
from ...runtime.durable_context import (
    DurableCallContext,
    DurableCallDiagnostics,
    run_durable_call_activity,
)
from ...runtime.environment.log import err_field, str_field
from ...runtime.schedule import normalize_temporal_priority


DURABLE_WORKFLOW_TYPE = "servicegen.durable-link.v1"
ENDPOINT_WORKFLOW_TYPE = "servicegen.temporal-endpoint.v1"
_MEMO_MANAGED_BY = "servicegen.managedBy"
_MEMO_OWNER = "servicegen.owner"
_MEMO_CALL_ID = "servicegen.callId"
_SCHEDULE_WORKFLOW_ID_SUFFIX = re.compile(
    r"-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$"
)
_SDK_METRICS_BIND_ADDRESS_ENVIRONMENT = "TEMPORAL_SDK_METRICS_BIND_ADDRESS"
_SDK_RUNTIME_LOCK = threading.Lock()
_SDK_RUNTIME: Runtime | None = None
_SDK_RUNTIME_ADDRESS: str | None = None


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
    execution_id: str
    stream_id: str
    priority: int
    deadline_unix_nano: int = 0
    sampling_enabled: bool = False
    scheduled: bool = False
    schedule_id: str = ""
    scheduled_at_unix_nano: int = 0
    fired_at_unix_nano: int = 0
    payload: bytes = b""
    trace_carrier: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EndpointResult:
    payload: bytes = b""


EndpointHandler = Callable[[EndpointEnvelope], Awaitable[EndpointResult]]


@dataclass(frozen=True, slots=True)
class _DurableWorkflowRequest:
    activity_type: str
    activity_start_to_close_millis: int
    activity_heartbeat_millis: int
    maximum_attempts: int
    priority: int
    envelope: DurableEnvelope


@dataclass(frozen=True, slots=True)
class _EndpointWorkflowRequest:
    activity_type: str
    activity_start_to_close_millis: int
    activity_heartbeat_millis: int
    maximum_attempts: int
    priority: int
    envelope: EndpointEnvelope


@workflow.defn(name=DURABLE_WORKFLOW_TYPE, sandboxed=False)
class _DurableLinkWorkflow:
    @workflow.run
    async def run(self, request: _DurableWorkflowRequest) -> None:
        await workflow.execute_activity(
            request.activity_type,
            request.envelope,
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


@workflow.defn(name=ENDPOINT_WORKFLOW_TYPE, sandboxed=False)
class _TemporalEndpointWorkflow:
    @workflow.run
    async def run(self, request: _EndpointWorkflowRequest) -> EndpointResult:
        envelope = request.envelope
        if envelope.scheduled:
            info = workflow.info()
            envelope = replace(
                envelope,
                execution_id=info.workflow_id,
                stream_id=info.workflow_id,
                scheduled_at_unix_nano=_scheduled_time_nanos(
                    info.workflow_id, info.workflow_start_time
                ),
            )
        return await workflow.execute_activity(
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
class _LinkRegistration:
    link_id: LinkId
    activity_type: str
    handler: Callable[[DurableEnvelope], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _EndpointRegistration:
    endpoint_id: int
    activity_type: str
    handler: Optional[EndpointHandler]


@dataclass(slots=True)
class _QueueRegistration:
    activities: list[Callable[..., Awaitable[Any]]]
    durable_workflow: bool = False
    endpoint_workflow: bool = False


def _required_string(obj: Any, name: str) -> str:
    value = getattr(obj, name, None)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{type(obj).__name__}.{name} must be a non-empty string")
    return value


def _integer(obj: Any, name: str, default: int = 0) -> int:
    value = getattr(obj, name, None)
    return value if isinstance(value, int) else default


def _bool(obj: Any, name: str, default: bool = False) -> bool:
    value = getattr(obj, name, None)
    return value if isinstance(value, bool) else default


class Connector(DurableTransport):
    """One official Temporal client and its generated Worker registrations."""

    def __init__(self, connector_id: int, environment: ServiceExecutionEnvironment):
        cfg = environment.config.get_data_connector_by_id(connector_id)
        self._id = connector_id
        self._name = cfg.name
        self._environment = environment
        self._links: dict[LinkId, _LinkRegistration] = {}
        self._endpoints: dict[int, _EndpointRegistration] = {}
        self._client: Optional[Client] = None
        self._workers: list[Worker] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._durable_events = environment.metrics.scope(
            "durable_call", {"connector": self._name}
        ).counter_vec(
            "events_total", "Total number of DurableCall Activity lifecycle events"
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
            self._durable_events.with_({
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
            if event in ("missing_outcome", "duplicate_terminal", "late_heartbeat"):
                self._environment.log.warn(
                    "DurableCall Activity lifecycle misuse", *fields
                )
            else:
                self._environment.log.error("DurableCall Activity failed", *fields)

        return report

    def register_link(
        self,
        link_id: LinkId,
        handler: Callable[[DurableEnvelope], Awaitable[None]],
    ) -> None:
        if self._started:
            raise RuntimeError("cannot register DurableCall after Temporal connector start")
        if link_id in self._links:
            raise ValueError(
                f"durable link {link_id.from_id}->{link_id.to_id} is already registered"
            )
        cfg = self._environment.config.get_link(link_id.from_id, link_id.to_id)
        if cfg is None or cfg.id_data_connector != self._id:
            raise ValueError(
                f"link {link_id.from_id}->{link_id.to_id} does not belong to "
                f"Temporal connector {self._name!r}"
            )
        service_id = self._environment.service_config.id
        self._links[link_id] = _LinkRegistration(
            link_id,
            f"servicegen.durable.{service_id}.{link_id.from_id}.{link_id.to_id}.v1",
            handler,
        )

    def register_endpoint(self, endpoint_id: int, handler: EndpointHandler) -> None:
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
        self._endpoints[endpoint_id] = replace(registration, handler=handler)

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
        inputs = [
            stream
            for stream in self._environment.config.streams
            if getattr(stream, "id_endpoint", None) == endpoint_id
            and stream.type == TransformationType.Input
        ]
        if len(inputs) != 1:
            raise ValueError(
                f"Temporal endpoint {endpoint_id} must have exactly one input stream; "
                f"found {len(inputs)}"
            )
        service_id = inputs[0].id_service
        return _EndpointRegistration(
            endpoint_id,
            f"servicegen.endpoint.{service_id}.{endpoint_id}.v1",
            None,
        )

    async def start(self, ctx: Context) -> None:
        if self._started:
            return
        cfg = self._config()
        tls = self._tls_config(cfg)
        connect = Client.connect(
            _required_string(cfg, "address"),
            namespace=_required_string(cfg, "namespace"),
            identity=getattr(cfg, "identity", None) or None,
            api_key=getattr(cfg, "api_key", None) or None,
            tls=tls,
            runtime=_sdk_runtime(),
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
                workflows: list[type[Any]] = []
                if registration.durable_workflow:
                    workflows.append(_DurableLinkWorkflow)
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
        for link_id, link_registration in self._links.items():
            cfg = self._link_config(link_id)
            queue = queues.setdefault(
                _required_string(cfg, "task_queue"), _QueueRegistration([])
            )

            queue.activities.append(
                _make_link_activity(
                    link_registration,
                    self._durable_diagnostics(
                        "link",
                        f"{link_id.from_id}:{link_id.to_id}",
                    ),
                )
            )
            queue.durable_workflow = True
        for endpoint_id, endpoint_registration in self._endpoints.items():
            if endpoint_registration.handler is None:
                continue
            cfg = self._endpoint_config(endpoint_id)
            if not _bool(cfg, "enabled"):
                continue
            queue = queues.setdefault(
                _required_string(cfg, "task_queue"), _QueueRegistration([])
            )

            queue.activities.append(
                _make_endpoint_activity(
                    endpoint_registration,
                    self._durable_diagnostics("schedule", str(endpoint_id)),
                )
            )
            queue.endpoint_workflow = True
        return queues

    async def submit_link(self, link_id: LinkId, envelope: DurableEnvelope) -> None:
        registration = self._links.get(link_id)
        if not self._started:
            raise RuntimeError(f"Temporal connector {self._name!r} is not started")
        client = self._require_client()
        if registration is None:
            raise ValueError(
                f"durable link {link_id.from_id}->{link_id.to_id} is not registered"
            )
        cfg = self._link_config(link_id)
        request = _DurableWorkflowRequest(
            registration.activity_type,
            _integer(cfg, "activity_start_to_close_timeout"),
            _integer(cfg, "activity_heartbeat_timeout"),
            _integer(cfg, "maximum_attempts"),
            normalize_temporal_priority(envelope.priority),
            envelope,
        )
        service_id = self._environment.service_config.id
        workflow_id = (
            f"servicegen/durable/{service_id}/{link_id.from_id}/"
            f"{link_id.to_id}/{envelope.call_id}"
        )
        owner = (
            f"servicegen/{service_id}/link/{link_id.from_id}/{link_id.to_id}/v1"
        )
        handle = await client.start_workflow(
            DURABLE_WORKFLOW_TYPE,
            request,
            id=workflow_id,
            task_queue=_required_string(cfg, "task_queue"),
            execution_timeout=_optional_timeout(cfg, "workflow_execution_timeout"),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            memo=_ownership_memo(owner, envelope.call_id),
            priority=Priority(priority_key=request.priority),
        )
        await _validate_workflow_ownership(
            handle, DURABLE_WORKFLOW_TYPE, owner, envelope.call_id
        )

    async def submit_endpoint(
        self,
        endpoint_id: int,
        envelope: EndpointEnvelope,
        wait_for_result: bool,
    ) -> EndpointResult:
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
        service_id = self._environment.service_config.id
        workflow_id = (
            f"servicegen/endpoint/{service_id}/{endpoint_id}/{envelope.execution_id}"
        )
        owner = f"servicegen/{service_id}/endpoint/{endpoint_id}/v1"
        handle = await client.start_workflow(
            ENDPOINT_WORKFLOW_TYPE,
            request,
            id=workflow_id,
            task_queue=_required_string(cfg, "task_queue"),
            execution_timeout=_optional_timeout(cfg, "workflow_execution_timeout"),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            memo=_ownership_memo(owner, envelope.execution_id),
            priority=Priority(priority_key=request.priority),
        )
        await _validate_workflow_ownership(
            handle, ENDPOINT_WORKFLOW_TYPE, owner, envelope.execution_id
        )
        if not wait_for_result:
            return EndpointResult()
        return cast(EndpointResult, await handle.result())

    async def _ensure_schedule(self, cfg: EndpointConfig) -> None:
        client = self._require_client()
        endpoint_id = cfg.id
        registration = self._endpoints[endpoint_id]
        schedule_id = _required_string(cfg, "schedule_id")
        service_id = self._environment.service_config.id
        owner = f"servicegen/{service_id}/endpoint/{endpoint_id}/v1"
        request = _EndpointWorkflowRequest(
            registration.activity_type,
            _integer(cfg, "activity_start_to_close_timeout"),
            _integer(cfg, "activity_heartbeat_timeout"),
            _integer(cfg, "maximum_attempts"),
            normalize_temporal_priority(0),
            EndpointEnvelope(
                version=1,
                endpoint_id=endpoint_id,
                execution_id="",
                stream_id="",
                priority=0,
                scheduled=True,
                schedule_id=schedule_id,
            ),
        )
        overlap = ScheduleOverlapPolicy.ALLOW_ALL
        if getattr(cfg, "overlap_policy", None) == ApiOverlapPolicy.SKIP:
            overlap = ScheduleOverlapPolicy.SKIP
        catchup = timedelta(seconds=10)
        if getattr(cfg, "missed_run_policy", None) == ScheduleMissedRunPolicy.FIREONCE:
            catchup = timedelta(days=365)
        action = ScheduleActionStartWorkflow(
            ENDPOINT_WORKFLOW_TYPE,
            request,
            id=f"servicegen/schedule/{service_id}/{endpoint_id}",
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
                        cron_expressions=[_required_string(cfg, "schedule")],
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
                or existing.workflow != ENDPOINT_WORKFLOW_TYPE
                or existing.task_queue != _required_string(cfg, "task_queue")
            ):
                raise RuntimeError(
                    f"Temporal schedule {schedule_id!r} ownership collision"
                ) from None

    def _link_config(self, link_id: LinkId) -> Any:
        cfg = self._environment.config.get_link(link_id.from_id, link_id.to_id)
        if cfg is None:
            raise ValueError(
                f"durable link {link_id.from_id}->{link_id.to_id} configuration not found"
            )
        return cfg

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


def _make_link_activity(
    registration: _LinkRegistration,
    diagnostics: DurableCallDiagnostics | None = None,
) -> Callable[[DurableEnvelope], Awaitable[None]]:
    async def invoke_link(envelope: DurableEnvelope) -> None:
        if (
            envelope.version != 1
            or envelope.from_id != registration.link_id.from_id
            or envelope.to_id != registration.link_id.to_id
            or not envelope.call_id
        ):
            raise ValueError(
                f"invalid durable envelope for link "
                f"{registration.link_id.from_id}->{registration.link_id.to_id}"
            )
        durable = DurableCallContext(
            envelope.call_id,
            heartbeat=activity.heartbeat,
            diagnostics=diagnostics,
        )
        await run_durable_call_activity(
            durable, lambda: registration.handler(envelope)
        )

    return activity.defn(name=registration.activity_type)(invoke_link)


def _make_endpoint_activity(
    registration: _EndpointRegistration,
    diagnostics: DurableCallDiagnostics | None = None,
) -> Callable[[EndpointEnvelope], Awaitable[EndpointResult]]:
    async def invoke_endpoint(envelope: EndpointEnvelope) -> EndpointResult:
        if (
            envelope.version != 1
            or envelope.endpoint_id != registration.endpoint_id
            or not envelope.execution_id
        ):
            raise ValueError(
                f"invalid durable envelope for Temporal endpoint "
                f"{registration.endpoint_id}"
            )
        handler = registration.handler
        if handler is None:
            raise RuntimeError(
                f"Temporal endpoint {registration.endpoint_id} has no local Activity handler"
            )
        fired = replace(envelope, fired_at_unix_nano=workflow_time_nanos())
        if not fired.scheduled:
            return await handler(fired)
        durable = DurableCallContext(
            fired.execution_id,
            heartbeat=activity.heartbeat,
            diagnostics=diagnostics,
        )
        result = EndpointResult()

        async def invoke() -> None:
            nonlocal result
            result = await handler(fired)

        await run_durable_call_activity(durable, invoke)
        return result

    return activity.defn(name=registration.activity_type)(invoke_endpoint)


def _optional_timeout(obj: Any, name: str) -> Optional[timedelta]:
    milliseconds = _integer(obj, name)
    return timedelta(milliseconds=milliseconds) if milliseconds > 0 else None


def _ownership_memo(owner: str, call_id: str) -> dict[str, str]:
    return {
        _MEMO_MANAGED_BY: "servicegen",
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
    existing = environment.get_durable_transport(connector_id)
    if existing is not None:
        if not isinstance(existing, Connector):
            raise TypeError(
                f"durable transport id={connector_id} is not a Python Temporal connector"
            )
        return existing
    connector = Connector(connector_id, environment)
    environment.add_durable_transport(connector)
    return connector

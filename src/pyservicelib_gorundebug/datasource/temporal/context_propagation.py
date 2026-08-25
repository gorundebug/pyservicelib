#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""ServiceLib MessageContext propagation through native Temporal Headers."""

from __future__ import annotations

from contextlib import ExitStack, nullcontext
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Optional

from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Interceptor as ClientInterceptor,
    OutboundInterceptor as ClientOutboundInterceptor,
    StartWorkflowInput,
)
from temporalio.converter import DefaultPayloadConverter
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor as WorkerInterceptor,
    StartActivityInput,
    StartChildWorkflowInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from temporalio import workflow

from ...runtime.context import request_deadline, request_priority, request_stream_id
from ...runtime.environment.tracing import (
    Tracing,
    sampling_enabled,
    sampling_requested_by_carrier,
    sampling_scope,
)


TEMPORAL_HEADER_STREAM_ID = "x-stream-id"
TEMPORAL_HEADER_PRIORITY = "servicelib-priority"
TEMPORAL_HEADER_DEADLINE_UNIX_NANO = "servicelib-deadline-unix-nano"
TEMPORAL_CARRIER_KEYS = (
    "traceparent",
    "tracestate",
    "baggage",
    "x-trace",
    TEMPORAL_HEADER_STREAM_ID,
    TEMPORAL_HEADER_PRIORITY,
    TEMPORAL_HEADER_DEADLINE_UNIX_NANO,
)


def _current_carrier(tracing: Optional[Tracing]) -> dict[str, str]:
    carrier: dict[str, str] = {}
    if tracing is not None:
        tracing.inject(carrier)
    if sampling_enabled():
        carrier["x-trace"] = "1"
    stream_id = request_stream_id.get()
    if stream_id:
        carrier[TEMPORAL_HEADER_STREAM_ID] = stream_id
    priority = request_priority.get()
    if priority is not None:
        carrier[TEMPORAL_HEADER_PRIORITY] = str(priority)
    deadline = request_deadline.get()
    if deadline is not None:
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        deadline = deadline.astimezone(timezone.utc)
        carrier[TEMPORAL_HEADER_DEADLINE_UNIX_NANO] = str(
            int(deadline.timestamp() * 1_000_000_000)
        )
    return carrier


def _encode_carrier(carrier: dict[str, str]) -> dict[str, Payload]:
    converter = DefaultPayloadConverter.default
    return {
        key: converter.to_payload(value)
        for key, value in carrier.items()
        if key in TEMPORAL_CARRIER_KEYS and value
    }


def _decode_carrier(headers: Mapping[str, Payload]) -> dict[str, str]:
    converter = DefaultPayloadConverter.default
    carrier: dict[str, str] = {}
    for key in TEMPORAL_CARRIER_KEYS:
        payload = headers.get(key)
        if payload is None:
            continue
        value = converter.from_payload(payload, str)
        if isinstance(value, str) and value:
            carrier[key] = value
    return carrier


def current_workflow_carrier() -> dict[str, str]:
    """Decode the canonical carrier attached to the current Workflow start."""

    return _decode_carrier(workflow.info().headers)


def _merge_headers(
    existing: Mapping[str, Payload],
    carrier_headers: dict[str, Payload],
) -> dict[str, Payload]:
    merged = dict(existing)
    merged.update(carrier_headers)
    return merged


class _ClientOutbound(ClientOutboundInterceptor):
    def __init__(self, next: ClientOutboundInterceptor, tracing: Optional[Tracing]) -> None:
        super().__init__(next)
        self._tracing = tracing

    async def start_workflow(self, input: StartWorkflowInput) -> Any:
        headers = _merge_headers(
            input.headers,
            _encode_carrier(_current_carrier(self._tracing)),
        )
        return await self.next.start_workflow(replace(input, headers=headers))


class _WorkflowOutbound(WorkflowOutboundInterceptor):
    @staticmethod
    def _root_headers() -> dict[str, Payload]:
        return {
            key: payload
            for key, payload in workflow.info().headers.items()
            if key in TEMPORAL_CARRIER_KEYS
        }

    def start_activity(self, input: StartActivityInput) -> Any:
        return self.next.start_activity(
            replace(
                input,
                headers=_merge_headers(input.headers, self._root_headers()),
            )
        )

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        return await self.next.start_child_workflow(
            replace(
                input,
                headers=_merge_headers(input.headers, self._root_headers()),
            )
        )


class _WorkflowInbound(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        self.next.init(_WorkflowOutbound(outbound))


class _ActivityInbound(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, tracing: Optional[Tracing]) -> None:
        super().__init__(next)
        self._tracing = tracing

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        carrier = _decode_carrier(input.headers)
        stream_id = carrier.get(TEMPORAL_HEADER_STREAM_ID) or None
        raw_priority = carrier.get(TEMPORAL_HEADER_PRIORITY)
        priority = int(raw_priority) if raw_priority is not None else None
        raw_deadline = carrier.get(TEMPORAL_HEADER_DEADLINE_UNIX_NANO)
        deadline = (
            datetime.fromtimestamp(int(raw_deadline) / 1_000_000_000, timezone.utc)
            if raw_deadline is not None
            else None
        )
        stream_token = request_stream_id.set(stream_id)
        priority_token = request_priority.set(priority)
        deadline_token = request_deadline.set(deadline)
        try:
            with ExitStack() as scopes:
                remote_sampled = scopes.enter_context(
                    self._tracing.extract(carrier)
                    if self._tracing is not None and carrier
                    else nullcontext(False)
                )
                scopes.enter_context(
                    sampling_scope(
                        sampling_enabled()
                        or sampling_requested_by_carrier(carrier)
                        or remote_sampled
                    )
                )
                return await self.next.execute_activity(input)
        finally:
            request_deadline.reset(deadline_token)
            request_priority.reset(priority_token)
            request_stream_id.reset(stream_token)


class TemporalContextPropagationInterceptor(ClientInterceptor, WorkerInterceptor):
    """Propagate one canonical carrier from Client through Workflow to Activity."""

    def __init__(self, tracing: Optional[Tracing]) -> None:
        self._tracing = tracing

    def intercept_client(self, next: ClientOutboundInterceptor) -> ClientOutboundInterceptor:
        return _ClientOutbound(next, self._tracing)

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _ActivityInbound(next, self._tracing)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        del input
        return _WorkflowInbound

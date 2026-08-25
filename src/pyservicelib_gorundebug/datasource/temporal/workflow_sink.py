#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Workflow-isolate consumers for symmetric Temporal sink streams."""

from __future__ import annotations

from datetime import timezone
from typing import Any, cast

from temporalio import workflow

from ...runtime.common import Consumer, TypedSinkStream, TypedSinkStreamWithResult
from ...runtime.context import (
    priority_from_context,
    request_deadline,
    stream_id_from_context,
)
from .workflow import (
    WORKFLOW_SUBMISSION,
    EndpointEnvelope,
    _identity_name,
    submit_endpoint_from_workflow,
)


class _WorkflowSinkConsumer[T, R, E](Consumer[T]):
    def __init__(
        self,
        stream: TypedSinkStream[T, E] | TypedSinkStreamWithResult[T, R, E],
        with_result: bool,
    ) -> None:
        self._stream = stream
        self._with_result = with_result

    async def consume(self, value: T) -> None:
        submission = WORKFLOW_SUBMISSION.get()
        if submission is None:
            raise RuntimeError("Temporal Workflow sink used outside Workflow execution")
        parent_id = stream_id_from_context() or workflow.info().workflow_id
        message_id = f"{parent_id}/{_identity_name(self._stream.name)}"
        deadline = request_deadline.get()
        deadline_nanos = 0
        if deadline is not None:
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            deadline_nanos = int(
                deadline.astimezone(timezone.utc).timestamp() * 1_000_000_000
            )
        input_serde = (
            self._stream.input_serde
            if isinstance(self._stream, TypedSinkStreamWithResult)
            else self._stream.serde
        )
        result = await submit_endpoint_from_workflow(
            submission,
            self._stream.endpoint_id,
            EndpointEnvelope(
                version=1,
                endpoint_id=self._stream.endpoint_id,
                message_id=message_id,
                stream_id=parent_id,
                priority=priority_from_context() or 0,
                deadline_unix_nano=deadline_nanos,
                payload=bytes(input_serde.serialize(value)),
            ),
        )
        if not self._with_result:
            return
        result_stream = cast(TypedSinkStreamWithResult[T, R, E], self._stream)
        await result_stream.consume_result(result_stream.serde.deserialize(result.payload))


def make_workflow_sink_endpoint_consumer[T, E](
    stream: TypedSinkStream[T, E],
) -> Consumer[T]:
    consumer = _WorkflowSinkConsumer[T, Any, E](stream, False)
    stream.set_sink_consumer(consumer)
    return consumer


def make_workflow_sink_endpoint_consumer_with_result[T, R, E](
    stream: TypedSinkStreamWithResult[T, R, E],
) -> Consumer[T]:
    consumer = _WorkflowSinkConsumer[T, R, E](stream, True)
    stream.set_sink_consumer(consumer)
    return consumer

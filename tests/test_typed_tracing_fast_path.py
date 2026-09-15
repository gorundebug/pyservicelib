"""Typed helpers and durable tracing must not evaluate metadata on the off path."""

from pathlib import Path

import pytest

from pyservicelib_gorundebug.runtime.durable_context import DurableCallContext
from pyservicelib_gorundebug.runtime.environment.tracing import (
    Attribute, NOOP_SPAN, Span, SpanContext, StatusCode, Tracer,
    sampling_scope, start_stream_span, string_attr,
)


class _Span(Span):
    def __init__(self) -> None:
        self.events: list[tuple[str, tuple[Attribute, ...]]] = []
        self.errors: list[Exception] = []
        self.statuses: list[tuple[int, str]] = []
        self.ended = 0

    def end(self) -> None:
        self.ended += 1

    def set_attributes(self, *attrs: Attribute) -> None:
        raise AssertionError("Unexpected attribute mutation")

    def record_error(self, err: Exception) -> None:
        self.errors.append(err)

    def set_status(self, code: int, description: str) -> None:
        self.statuses.append((code, description))

    def add_event(self, name: str, *attrs: Attribute) -> None:
        self.events.append((name, attrs))

    def span_context(self) -> SpanContext:
        return SpanContext()


class _Tracer(Tracer):
    def __init__(self) -> None:
        self.span = _Span()
        self.calls: list[tuple[str, tuple[Attribute, ...]]] = []

    def start(self, span_name: str, *attrs: Attribute) -> tuple[None, Span]:
        self.calls.append((span_name, attrs))
        return None, self.span


class _Stream:
    def __init__(self) -> None:
        self.reads = 0
        self.attributes = (string_attr("stream", "Price"), string_attr("pipeline", "checkout"))

    @property
    def trace_attributes(self) -> tuple[Attribute, ...]:
        self.reads += 1
        return self.attributes


class _Error(Exception):
    def __init__(self) -> None:
        super().__init__("failed")
        self.formats = 0

    def __str__(self) -> str:
        self.formats += 1
        return "failed"


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("sampled", [False, True])
def test_typed_stream_helper_reads_only_cached_attributes_when_recording(present: bool, sampled: bool) -> None:
    tracer = _Tracer()
    stream = _Stream()
    with sampling_scope(sampled):
        _, span = start_stream_span(tracer if present else None, "stream.map", stream)
    enabled = present and sampled
    assert stream.reads == int(enabled)
    assert len(tracer.calls) == int(enabled)
    assert span is (tracer.span if enabled else NOOP_SPAN)
    if enabled:
        assert tracer.calls[0][1][0] is stream.attributes[0]
        assert tracer.calls[0][1][1] is stream.attributes[1]


@pytest.mark.parametrize("bind_noop", [False, True])
def test_durable_disabled_tracing_does_not_format_errors(bind_noop: bool) -> None:
    reports: list[tuple[str, BaseException | None]] = []

    def report(event: str, error: BaseException | None) -> None:
        reports.append((event, error))

    durable = DurableCallContext("message", diagnostics=report)
    if bind_noop:
        durable.bind_span(NOOP_SPAN)
    error = _Error()
    durable.close(error)
    assert error.formats == 0
    assert reports == [("error", error)]


def test_durable_recording_formats_error_once_and_preserves_identity() -> None:
    durable = DurableCallContext("message")
    span = _Span()
    durable.bind_span(span)
    error = _Error()
    durable.close(error)
    assert error.formats == 1
    assert span.errors == [error]
    assert span.events == [("temporal.activity.error", (string_attr("error", "failed"),))]
    assert span.statuses == [(StatusCode.ERROR, "failed")]
    assert span.ended == 1
    durable.close(error)
    assert error.formats == 1
    assert span.ended == 1


def test_heartbeat_and_success_keep_existing_event_names_without_attributes() -> None:
    durable = DurableCallContext("message")
    span = _Span()
    durable.bind_span(span)
    durable.heartbeat("progress")
    durable.close(None)
    assert span.events == [("temporal.activity.heartbeat", ()), ("temporal.activity.success", ())]
    assert span.statuses == [(StatusCode.OK, "")]
    assert span.ended == 1


def test_tracing_contracts_do_not_fall_back_to_any_or_getattr() -> None:
    root = Path(__file__).parents[1] / "src" / "pyservicelib_gorundebug" / "runtime"
    for relative in ("environment/tracing/tracing.py", "telemetry/opentelemetry/opentelemetrytracing.py"):
        source = (root / relative).read_text()
        assert "Any" not in source, relative
        assert "getattr(" not in source, relative

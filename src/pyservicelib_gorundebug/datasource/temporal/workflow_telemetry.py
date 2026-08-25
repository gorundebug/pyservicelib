#  Copyright (c) 2026 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See LICENSE for details.

"""Replay-safe ServiceLib telemetry adapters for Temporal Workflows."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry import trace as otel_trace
from temporalio import workflow
from temporalio.common import (
    MetricCounter,
    MetricGauge,
    MetricHistogramFloat,
    MetricMeter,
)

from ...runtime.environment.log import Field, Logger
from ...runtime.environment.metrics import (
    Float64Histogram,
    Float64HistogramVec,
    Int64Counter,
    Int64CounterVec,
    Int64Gauge,
    Int64GaugeVec,
    Labels,
    Metrics,
    MetricsScope,
)
from ...runtime.environment.tracing import (
    Attribute,
    Span,
    SpanContext,
    StatusCode,
    Tracer,
    Tracing,
)
from ...runtime.execution_policy import recording_enabled


def _metric_name(prefix: str, name: str) -> str:
    return f"{prefix}_{name}" if name else prefix


def _merge_labels(base: Labels, extra: Labels) -> Labels:
    return base if not extra else {**base, **extra}


def _labels_key(labels: Labels) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(labels.items()))


class _Counter(Int64Counter):
    def __init__(self, metric: MetricCounter, labels: Labels) -> None:
        self._metric = metric
        self._labels = labels

    def inc(self) -> None:
        self.add(1)

    def add(self, v: int) -> None:
        if recording_enabled():
            self._metric.add(v, self._labels)


class _CounterVec(Int64CounterVec):
    def __init__(self, metric: MetricCounter, base: Labels) -> None:
        self._metric = metric
        self._base = base

    def with_(self, labels: Labels) -> Int64Counter:
        return _Counter(self._metric, _merge_labels(self._base, labels))


class _Gauge(Int64Gauge):
    def __init__(self, metric: MetricGauge, labels: Labels) -> None:
        self._metric = metric
        self._labels = labels
        self._value = 0

    def set(self, v: int) -> None:
        self._value = v
        if recording_enabled():
            self._metric.set(v, self._labels)

    def inc(self) -> None:
        self.add(1)

    def dec(self) -> None:
        self.sub(1)

    def add(self, delta: int) -> None:
        self.set(self._value + delta)

    def sub(self, delta: int) -> None:
        self.set(self._value - delta)


class _GaugeVec(Int64GaugeVec):
    def __init__(self, metric: MetricGauge, base: Labels) -> None:
        self._metric = metric
        self._base = base
        self._values: dict[tuple[tuple[str, str], ...], _Gauge] = {}

    def with_(self, labels: Labels) -> Int64Gauge:
        merged = _merge_labels(self._base, labels)
        key = _labels_key(merged)
        gauge = self._values.get(key)
        if gauge is None:
            gauge = _Gauge(self._metric, merged)
            self._values[key] = gauge
        return gauge

    def delete(self, labels: Labels) -> None:
        merged = _merge_labels(self._base, labels)
        gauge = self._values.pop(_labels_key(merged), None)
        if gauge is not None:
            gauge.set(0)


class _Histogram(Float64Histogram):
    def __init__(self, metric: MetricHistogramFloat, labels: Labels) -> None:
        self._metric = metric
        self._labels = labels

    def observe(self, v: float) -> None:
        if recording_enabled():
            self._metric.record(v, self._labels)


class _HistogramVec(Float64HistogramVec):
    def __init__(self, metric: MetricHistogramFloat, base: Labels) -> None:
        self._metric = metric
        self._base = base

    def with_(self, labels: Labels) -> Float64Histogram:
        return _Histogram(self._metric, _merge_labels(self._base, labels))


class _MetricsScope(MetricsScope):
    def __init__(
        self,
        meter: MetricMeter,
        prefix: str,
        base: Labels,
        observables: list[Callable[[], None]],
    ) -> None:
        self._meter = meter
        self._prefix = prefix
        self._base = base
        self._observables = observables

    def counter(self, name: str, help: str, labels: Labels) -> Int64Counter:
        metric = self._meter.create_counter(_metric_name(self._prefix, name), help)
        return _Counter(metric, _merge_labels(self._base, labels))

    def counter_vec(self, name: str, help: str) -> Int64CounterVec:
        metric = self._meter.create_counter(_metric_name(self._prefix, name), help)
        return _CounterVec(metric, self._base)

    def gauge(self, name: str, help: str, labels: Labels) -> Int64Gauge:
        metric = self._meter.create_gauge(_metric_name(self._prefix, name), help)
        return _Gauge(metric, _merge_labels(self._base, labels))

    def gauge_vec(self, name: str, help: str) -> Int64GaugeVec:
        metric = self._meter.create_gauge(_metric_name(self._prefix, name), help)
        return _GaugeVec(metric, self._base)

    def histogram(
        self, name: str, help: str, labels: Labels, *buckets: float
    ) -> Float64Histogram:
        del buckets
        metric = self._meter.create_histogram_float(
            _metric_name(self._prefix, name), help
        )
        return _Histogram(metric, _merge_labels(self._base, labels))

    def histogram_vec(
        self, name: str, help: str, *buckets: float
    ) -> Float64HistogramVec:
        del buckets
        metric = self._meter.create_histogram_float(
            _metric_name(self._prefix, name), help
        )
        return _HistogramVec(metric, self._base)

    def observable_float64_gauge(
        self, name: str, help: str, fn: Callable[[], float]
    ) -> None:
        metric = self._meter.create_gauge_float(
            _metric_name(self._prefix, name), help
        )
        self._observables.append(
            lambda: metric.set(fn(), self._base) if recording_enabled() else None
        )


class WorkflowMetrics(Metrics):
    """ServiceLib metrics backed by Temporal's replay-safe Workflow meter."""

    def __init__(self) -> None:
        self._meter = workflow.metric_meter()
        self._observables: list[Callable[[], None]] = []

    @property
    def enabled(self) -> bool:
        return recording_enabled()

    def scope(self, prefix: str, labels: Labels) -> MetricsScope:
        return _MetricsScope(self._meter, prefix, labels, self._observables)

    def flush_observables(self) -> None:
        for observe in self._observables:
            observe()


class WorkflowLogger(Logger):
    """Structured adapter around Temporal's replay-aware Workflow logger."""

    @staticmethod
    def _extra(fields: tuple[Field, ...]) -> dict[str, Any]:
        return {"servicelib": {field.key: field.value() for field in fields}}

    def debug(self, msg: str, *fields: Field) -> None:
        workflow.logger.debug(msg, extra=self._extra(fields))

    def info(self, msg: str, *fields: Field) -> None:
        workflow.logger.info(msg, extra=self._extra(fields))

    def warn(self, msg: str, *fields: Field) -> None:
        workflow.logger.warning(msg, extra=self._extra(fields))

    def error(self, msg: str, *fields: Field) -> None:
        workflow.logger.error(msg, extra=self._extra(fields))


def _otel_attrs(attrs: tuple[Attribute, ...]) -> dict[str, Any]:
    return {attribute.key: attribute.value for attribute in attrs}


class _WorkflowSpan(Span):
    def __init__(self, span: otel_trace.Span) -> None:
        self._span = span

    def end(self) -> None:
        self._span.end()

    def set_attributes(self, *attrs: Attribute) -> None:
        self._span.set_attributes(_otel_attrs(attrs))

    def record_error(self, err: Exception) -> None:
        self._span.record_exception(err)

    def set_status(self, code: int, description: str) -> None:
        if code == StatusCode.OK:
            status = otel_trace.StatusCode.OK
        elif code == StatusCode.ERROR:
            status = otel_trace.StatusCode.ERROR
        else:
            status = otel_trace.StatusCode.UNSET
        self._span.set_status(otel_trace.Status(status, description))

    def add_event(self, name: str, *attrs: Attribute) -> None:
        self._span.add_event(name, _otel_attrs(attrs))

    def span_context(self) -> SpanContext:
        context = self._span.get_span_context()
        if not context.is_valid:
            return SpanContext()
        return SpanContext(
            trace_id=f"{context.trace_id:032x}",
            span_id=f"{context.span_id:016x}",
            is_valid=True,
        )

    @contextmanager
    def scoped(self) -> Iterator[Span]:
        token = otel_context.attach(otel_trace.set_span_in_context(self._span))
        try:
            yield self
        finally:
            otel_context.detach(token)


class _WorkflowTracer(Tracer):
    def __init__(self, tracer: otel_trace.Tracer) -> None:
        self._tracer = tracer

    def start(self, span_name: str, *attrs: Attribute) -> tuple[Any, Span]:
        span = self._tracer.start_span(span_name, attributes=_otel_attrs(attrs))
        return None, _WorkflowSpan(span)


class WorkflowTracing(Tracing):
    """ServiceLib tracing over Temporal's replay-safe OTel provider."""

    def tracer(self, name: str) -> Tracer:
        return _WorkflowTracer(otel_trace.get_tracer(name))

    @contextmanager
    def extract(self, carrier: Mapping[str, str]) -> Iterator[bool]:
        context = propagate.extract(dict(carrier))
        token = otel_context.attach(context)
        try:
            span_context = otel_trace.get_current_span().get_span_context()
            yield bool(span_context.trace_flags & otel_trace.TraceFlags.SAMPLED)
        finally:
            otel_context.detach(token)

    def inject(self, carrier: MutableMapping[str, str]) -> None:
        propagate.inject(carrier)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from contextlib import contextmanager
from typing import Any, Iterator, Mapping, MutableMapping, Tuple

from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_ON, ALWAYS_OFF, ParentBased, SamplingResult, Decision, Sampler,
)
from temporalio.contrib.opentelemetry import create_tracer_provider

from ...environment.tracing.tracing import (
    Attribute, SpanContext, StatusCode,
    Span, Tracer, Tracing, TracingEngine,
    sampling_enabled,
)


# ── OTel attribute conversion ─────────────────────────────────────────────────

def _to_otel_attr(a: Attribute):
    from opentelemetry import trace as otel_trace
    from opentelemetry.util.types import Attributes
    return (a.key, a.value)


def _to_otel_attrs(attrs: tuple[Attribute, ...]) -> dict:
    return {a.key: a.value for a in attrs}


# ── Span ──────────────────────────────────────────────────────────────────────

class _Span(Span):
    __slots__ = ('_span',)

    def __init__(self, span):
        self._span = span

    @contextmanager
    def scoped(self):
        ctx = otel_trace.set_span_in_context(self._span)
        token = otel_context.attach(ctx)
        try:
            yield self
        finally:
            otel_context.detach(token)

    def end(self) -> None:
        self._span.end()

    def set_attributes(self, *attrs: Attribute) -> None:
        self._span.set_attributes(_to_otel_attrs(attrs))

    def record_error(self, err: Exception) -> None:
        self._span.record_exception(err)

    def set_status(self, code: int, description: str) -> None:
        from opentelemetry.trace import StatusCode as OtelStatusCode
        if code == StatusCode.OK:
            self._span.set_status(otel_trace.Status(OtelStatusCode.OK, description))
        elif code == StatusCode.ERROR:
            self._span.set_status(otel_trace.Status(OtelStatusCode.ERROR, description))
        else:
            self._span.set_status(otel_trace.Status(OtelStatusCode.UNSET, description))

    def add_event(self, name: str, *attrs: Attribute) -> None:
        self._span.add_event(name, attributes=_to_otel_attrs(attrs))

    def span_context(self) -> SpanContext:
        sc = self._span.get_span_context()
        if sc is None or not sc.is_valid:
            return SpanContext()
        return SpanContext(
            trace_id=format(sc.trace_id, '032x'),
            span_id=format(sc.span_id, '016x'),
            is_valid=sc.is_valid,
        )


# ── Tracer ────────────────────────────────────────────────────────────────────

class _Tracer(Tracer):
    __slots__ = ('_tracer',)

    def __init__(self, tracer):
        self._tracer = tracer

    def start(self, span_name: str, *attrs: Attribute) -> Tuple[Any, Span]:
        span = self._tracer.start_span(span_name, attributes=_to_otel_attrs(attrs))
        return None, _Span(span)


# ── Tracing ───────────────────────────────────────────────────────────────────

class _Tracing(Tracing):
    __slots__ = ('_provider',)

    def __init__(self, provider: Any):
        self._provider = provider

    def tracer(self, name: str) -> Tracer:
        return _Tracer(self._provider.get_tracer(name))

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


# ── ContextSampler ────────────────────────────────────────────────────────────

class _ContextSampler(Sampler):
    """Records spans only when tracing.enable_sampling() was called in the current coroutine."""

    def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None) -> SamplingResult:
        if sampling_enabled():
            return SamplingResult(Decision.RECORD_AND_SAMPLE)
        return SamplingResult(Decision.DROP)

    def get_description(self) -> str:
        return 'ContextSampler'


# ── TracingEngine ─────────────────────────────────────────────────────────────

class OtelTracingEngine(TracingEngine):

    def __init__(self, provider: Any):
        self._provider = provider
        self._tracing = _Tracing(provider)

    @property
    def tracing(self) -> Tracing:
        return self._tracing

    async def shutdown(self) -> None:
        self._provider.shutdown()


def _build_provider(exporter, service_name: str, context_sampler: bool, sync: bool) -> Any:
    sampler = ParentBased(_ContextSampler()) if context_sampler else ALWAYS_ON
    resource = Resource({'service.name': service_name})
    # The official Temporal provider uses deterministic span identifiers while
    # Workflow code is replaying and behaves like the regular SDK provider in
    # process/Activity code.  Keeping a single provider preserves the ordinary
    # ServiceLib trace tree without introducing a second telemetry stack.
    provider = create_tracer_provider(sampler=sampler, resource=resource)
    if sync:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        provider.add_span_processor(BatchSpanProcessor(exporter))
    otel_trace.set_tracer_provider(provider)
    return provider


def create_stdout_tracing_engine(service_name: str, context_sampler: bool = True) -> TracingEngine:
    """Create a TracingEngine that prints spans to stdout as JSON."""
    exporter = ConsoleSpanExporter()
    return OtelTracingEngine(_build_provider(exporter, service_name, context_sampler, sync=True))


def create_otlp_tracing_engine(service_name: str, endpoint: str = 'localhost:4317',
                                insecure: bool = True, context_sampler: bool = True) -> TracingEngine:
    """Create a TracingEngine that exports spans via OTLP gRPC."""
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
    return OtelTracingEngine(_build_provider(exporter, service_name, context_sampler, sync=False))


def create_pretty_tracing_engine(service_name: str, context_sampler: bool = True) -> TracingEngine:
    """Create a TracingEngine that prints spans in human-readable single-line format."""
    from .prettytracing import PrettySpanExporter
    exporter = PrettySpanExporter()
    return OtelTracingEngine(_build_provider(exporter, service_name, context_sampler, sync=True))

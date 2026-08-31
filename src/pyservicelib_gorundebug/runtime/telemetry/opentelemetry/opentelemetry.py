#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import threading
from typing import Any, Callable, Iterable, Optional

import prometheus_client
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.metrics import Observation, CallbackOptions

from ...environment.metrics.metrics import (
    Labels, MetricsHandler,
    Int64Counter, Int64CounterVec,
    Float64Counter, Float64CounterVec,
    Int64Gauge, Int64GaugeVec,
    Float64Gauge, Float64GaugeVec,
    Float64Histogram, Float64HistogramVec,
    Int64Histogram, Int64HistogramVec,
    MetricsScope, Metrics, MetricsEngine,
    DEFAULT_MAX_CARDINALITY,
    NOOP_INT64_GAUGE, NOOP_FLOAT64_GAUGE,
)
from ...execution_policy import recording_enabled


def _labels_key(labels: Labels) -> str:
    return '\x00'.join(f'{k}={v}' for k, v in sorted(labels.items()))


def _to_otel_attrs(labels: Labels) -> dict[str, str]:
    return labels


# ── Int64Counter ──────────────────────────────────────────────────────────────

class _Int64Counter(Int64Counter):
    __slots__ = ('_counter', '_attrs')

    def __init__(self, counter, attrs: dict):
        self._counter = counter
        self._attrs = attrs

    def inc(self) -> None:
        if not recording_enabled():
            return
        self._counter.add(1, self._attrs)

    def add(self, v: int) -> None:
        if not recording_enabled():
            return
        self._counter.add(v, self._attrs)


class _Int64CounterVec(Int64CounterVec):
    __slots__ = ('_counter',)

    def __init__(self, counter):
        self._counter = counter

    def with_(self, labels: Labels) -> Int64Counter:
        return _Int64Counter(self._counter, labels)


# ── Float64Counter ────────────────────────────────────────────────────────────

class _Float64Counter(Float64Counter):
    __slots__ = ('_counter', '_attrs')

    def __init__(self, counter, attrs: dict):
        self._counter = counter
        self._attrs = attrs

    def add(self, v: float) -> None:
        if not recording_enabled():
            return
        self._counter.add(v, self._attrs)


class _Float64CounterVec(Float64CounterVec):
    __slots__ = ('_counter',)

    def __init__(self, counter):
        self._counter = counter

    def with_(self, labels: Labels) -> Float64Counter:
        return _Float64Counter(self._counter, labels)


# ── Int64Gauge ────────────────────────────────────────────────────────────────

class _Int64Gauge(Int64Gauge):
    __slots__ = ('_val', '_lock', '_attrs')

    def __init__(self, attrs: dict):
        self._val = 0
        self._lock = threading.Lock()
        self._attrs = attrs

    def _read(self) -> int:
        with self._lock:
            return self._val

    def set(self, v: int) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val = v

    def inc(self) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val += 1

    def dec(self) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val -= 1

    def add(self, delta: int) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val += delta

    def sub(self, delta: int) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val -= delta


class _Int64GaugeVec(Int64GaugeVec):
    __slots__ = ('_instances', '_lock', '_count', '_dropped', '_max_cardinality', '_obs')

    def __init__(self, obs, max_cardinality: int):
        self._instances: dict[str, _Int64Gauge] = {}
        self._lock = threading.Lock()
        self._count = 0
        self._dropped = 0
        self._max_cardinality = max_cardinality
        self._obs = obs

    def _iter_observations(self) -> Iterable[Observation]:
        with self._lock:
            items = list(self._instances.values())
        for g in items:
            yield Observation(g._read(), g._attrs)

    def with_(self, labels: Labels) -> Int64Gauge:
        key = _labels_key(labels)
        with self._lock:
            if key in self._instances:
                return self._instances[key]
            if 0 < self._max_cardinality <= self._count:
                self._dropped += 1
                return NOOP_INT64_GAUGE
            g = _Int64Gauge(labels)
            self._instances[key] = g
            self._count += 1
            return g

    def delete(self, labels: Labels) -> None:
        key = _labels_key(labels)
        with self._lock:
            if key in self._instances:
                del self._instances[key]
                self._count -= 1


# ── Float64Gauge ──────────────────────────────────────────────────────────────

class _Float64Gauge(Float64Gauge):
    __slots__ = ('_val', '_lock', '_attrs')

    def __init__(self, attrs: dict):
        self._val = 0.0
        self._lock = threading.Lock()
        self._attrs = attrs

    def _read(self) -> float:
        with self._lock:
            return self._val

    def set(self, v: float) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val = v

    def inc(self) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val += 1.0

    def dec(self) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val -= 1.0

    def add(self, delta: float) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val += delta

    def sub(self, delta: float) -> None:
        if not recording_enabled():
            return
        with self._lock:
            self._val -= delta


class _Float64GaugeVec(Float64GaugeVec):
    __slots__ = ('_instances', '_lock', '_count', '_dropped', '_max_cardinality', '_obs')

    def __init__(self, obs, max_cardinality: int):
        self._instances: dict[str, _Float64Gauge] = {}
        self._lock = threading.Lock()
        self._count = 0
        self._dropped = 0
        self._max_cardinality = max_cardinality
        self._obs = obs

    def _iter_observations(self) -> Iterable[Observation]:
        with self._lock:
            items = list(self._instances.values())
        for g in items:
            yield Observation(g._read(), g._attrs)

    def with_(self, labels: Labels) -> Float64Gauge:
        key = _labels_key(labels)
        with self._lock:
            if key in self._instances:
                return self._instances[key]
            if 0 < self._max_cardinality <= self._count:
                self._dropped += 1
                return NOOP_FLOAT64_GAUGE
            g = _Float64Gauge(labels)
            self._instances[key] = g
            self._count += 1
            return g

    def delete(self, labels: Labels) -> None:
        key = _labels_key(labels)
        with self._lock:
            if key in self._instances:
                del self._instances[key]
                self._count -= 1


class _ObservableFloat64GaugeVec:
    __slots__ = ('_callbacks', '_lock', '_obs')

    def __init__(self) -> None:
        self._callbacks: dict[str, tuple[dict[str, str], Callable[[], float]]] = {}
        self._lock = threading.Lock()
        self._obs = None

    def add(self, labels: Labels, fn: Callable[[], float]) -> None:
        key = _labels_key(labels)
        with self._lock:
            if key in self._callbacks:
                raise ValueError(
                    f'observable gauge already registered for labels {labels!r}'
                )
            self._callbacks[key] = (dict(labels), fn)

    def observations(self) -> list[Observation]:
        with self._lock:
            callbacks = list(self._callbacks.values())
        return [Observation(fn(), labels) for labels, fn in callbacks]


# ── Float64Histogram ──────────────────────────────────────────────────────────

class _Float64Histogram(Float64Histogram):
    __slots__ = ('_hist', '_attrs')

    def __init__(self, hist, attrs: dict):
        self._hist = hist
        self._attrs = attrs

    def observe(self, v: float) -> None:
        if not recording_enabled():
            return
        self._hist.record(v, self._attrs)


class _Float64HistogramVec(Float64HistogramVec):
    __slots__ = ('_hist',)

    def __init__(self, hist):
        self._hist = hist

    def with_(self, labels: Labels) -> Float64Histogram:
        return _Float64Histogram(self._hist, labels)


# ── Int64Histogram ────────────────────────────────────────────────────────────

class _Int64Histogram(Int64Histogram):
    __slots__ = ('_hist', '_attrs')

    def __init__(self, hist, attrs: dict):
        self._hist = hist
        self._attrs = attrs

    def observe(self, v: int) -> None:
        if not recording_enabled():
            return
        self._hist.record(v, self._attrs)


class _Int64HistogramVec(Int64HistogramVec):
    __slots__ = ('_hist',)

    def __init__(self, hist):
        self._hist = hist

    def with_(self, labels: Labels) -> Int64Histogram:
        return _Int64Histogram(self._hist, labels)


# ── MetricsScope ──────────────────────────────────────────────────────────────

class _MetricsScope(MetricsScope):
    __slots__ = ('_otel', '_prefix', '_base')

    def __init__(self, otel: '_OtelMetrics', prefix: str, base: Labels):
        self._otel = otel
        self._prefix = prefix
        self._base = base

    def _full_name(self, name: str) -> str:
        return f'{self._prefix}_{name}' if name else self._prefix

    def _merge(self, extra: Labels) -> Labels:
        if not extra:
            return self._base
        return {**self._base, **extra}

    def counter(self, name: str, help: str, labels: Labels) -> Int64Counter:
        full = self._full_name(name)
        c = self._otel.meter.create_counter(full, description=help)
        return _Int64Counter(c, self._merge(labels))

    def counter_vec(self, name: str, help: str) -> Int64CounterVec:
        full = self._full_name(name)
        c = self._otel.meter.create_counter(full, description=help)
        return _Int64CounterVec(c)

    def gauge(self, name: str, help: str, labels: Labels) -> Int64Gauge:
        full = self._full_name(name)
        merged = self._merge(labels)
        return self._otel.gauge_vec(full, help).with_(merged)

    def gauge_vec(self, name: str, help: str) -> Int64GaugeVec:
        full = self._full_name(name)
        gv = _Int64GaugeVec(None, DEFAULT_MAX_CARDINALITY)
        obs = self._otel.meter.create_observable_gauge(
            full, description=help,
            callbacks=[lambda _opts, _gv=gv: list(_gv._iter_observations())],  # type: ignore[misc]
        )
        gv._obs = obs
        return gv

    def histogram(self, name: str, help: str, labels: Labels, *buckets: float) -> Float64Histogram:
        full = self._full_name(name)
        boundaries = list(buckets) if buckets else None
        kwargs: dict[str, Any] = {'description': help}
        if boundaries:
            kwargs['explicit_bucket_boundaries_advisory'] = boundaries
        h = self._otel.meter.create_histogram(full, **kwargs)
        return _Float64Histogram(h, self._merge(labels))

    def histogram_vec(self, name: str, help: str, *buckets: float) -> Float64HistogramVec:
        full = self._full_name(name)
        kwargs: dict[str, Any] = {'description': help}
        if buckets:
            kwargs['explicit_bucket_boundaries_advisory'] = list(buckets)
        h = self._otel.meter.create_histogram(full, **kwargs)
        return _Float64HistogramVec(h)

    def observable_float64_gauge(self, name: str, help: str, fn: Callable[[], float]) -> None:
        full = self._full_name(name)
        self._otel.observable_float64_gauge(full, help, self._base, fn)


# ── OtelMetrics ───────────────────────────────────────────────────────────────

class _OtelMetrics(Metrics):
    __slots__ = ('meter', '_scopes', '_gauges', '_observable_gauges', '_lock')

    def __init__(self, provider: MeterProvider):
        self.meter = provider.get_meter('github.com/gorundebug/servicelib')
        self._scopes: dict[str, _MetricsScope] = {}
        self._gauges: dict[str, _Int64GaugeVec] = {}
        self._observable_gauges: dict[
            str, tuple[str, _ObservableFloat64GaugeVec]
        ] = {}
        self._lock = threading.Lock()

    def gauge_vec(self, name: str, help: str) -> _Int64GaugeVec:
        with self._lock:
            gauge = self._gauges.get(name)
            if gauge is None:
                gauge = _Int64GaugeVec(None, DEFAULT_MAX_CARDINALITY)
                observable = self.meter.create_observable_gauge(
                    name,
                    description=help,
                    callbacks=[
                        lambda _opts, _gauge=gauge: list(  # type: ignore[misc]
                            _gauge._iter_observations()
                        )
                    ],
                )
                gauge._obs = observable
                self._gauges[name] = gauge
            return gauge

    def observable_float64_gauge(
        self,
        name: str,
        help: str,
        labels: Labels,
        fn: Callable[[], float],
    ) -> None:
        with self._lock:
            entry = self._observable_gauges.get(name)
            if entry is None:
                gauge = _ObservableFloat64GaugeVec()
                observable = self.meter.create_observable_gauge(
                    name,
                    description=help,
                    callbacks=[
                        lambda _opts, _gauge=gauge: _gauge.observations()  # type: ignore[misc]
                    ],
                )
                gauge._obs = observable
                self._observable_gauges[name] = (help, gauge)
            else:
                registered_help, gauge = entry
                if registered_help != help:
                    raise ValueError(
                        f'observable gauge {name!r} is already registered '
                        'with different help text'
                    )
            gauge.add(labels, fn)

    def scope(self, prefix: str, labels: Labels) -> MetricsScope:
        key = f'{prefix}\x00{_labels_key(labels)}'
        with self._lock:
            if key not in self._scopes:
                self._scopes[key] = _MetricsScope(self, prefix, labels)
            return self._scopes[key]


# ── PrometheusMetricsEngine ───────────────────────────────────────────────────

class PrometheusMetricsEngine(MetricsEngine):

    def __init__(self, service_name: str):
        self._registry = prometheus_client.CollectorRegistry()
        # Register the standard prometheus_client runtime collectors in the
        # same explicit registry used by the OpenTelemetry Prometheus reader.
        # This keeps generated Python runtime dashboards truthful without
        # introducing framework-specific process or GC approximations.
        prometheus_client.ProcessCollector(registry=self._registry)
        prometheus_client.PlatformCollector(registry=self._registry)
        prometheus_client.GCCollector(registry=self._registry)
        reader = PrometheusMetricReader(registry=self._registry)
        provider = MeterProvider(
            metric_readers=[reader],
            resource=Resource.create({'service.name': service_name}),
        )
        self._otel_metrics = _OtelMetrics(provider)
        self._provider = provider

    @property
    def metrics(self) -> Metrics:
        return self._otel_metrics

    @property
    def metrics_handler(self) -> MetricsHandler:
        registry = self._registry
        return lambda: prometheus_client.generate_latest(registry)

    async def shutdown(self) -> None:
        self._provider.shutdown()


# ── OTLPMetricsEngine ─────────────────────────────────────────────────────────

class OTLPMetricsEngine(MetricsEngine):

    def __init__(self, service_name: str, endpoint: str = 'localhost:4317', insecure: bool = True):
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=insecure)
        reader = PeriodicExportingMetricReader(exporter)
        provider = MeterProvider(
            metric_readers=[reader],
            resource=Resource.create({'service.name': service_name}),
        )
        self._otel_metrics = _OtelMetrics(provider)
        self._provider = provider

    @property
    def metrics(self) -> Metrics:
        return self._otel_metrics

    @property
    def metrics_handler(self) -> MetricsHandler:
        return lambda: b''

    async def shutdown(self) -> None:
        self._provider.shutdown()

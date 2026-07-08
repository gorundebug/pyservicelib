#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""In-memory metrics engine for tests.

Usage::

    engine = TestMetrics()
    # pass via ServiceDependency.get_metrics_engine()

    send_request()

    Expect(engine).counter("stream_messages_total") \\
        .with_("from", "InputRequest").eq(1)
    Expect(engine).gauge("service_info") \\
        .with_("service", "IncomeService").eq(1)
"""

import threading
from typing import Callable, Optional

from ..environment.metrics import (
    Labels, MetricsHandler, MetricsEngine, Metrics, MetricsScope,
    Int64Counter, Int64CounterVec,
    Float64Counter, Float64CounterVec,
    Int64Gauge, Int64GaugeVec,
    Float64Gauge, Float64GaugeVec,
    Float64Histogram, Float64HistogramVec,
    Int64Histogram, Int64HistogramVec,
)


# ── TestInt64Counter ──────────────────────────────────────────────────────────

class TestInt64Counter(Int64Counter):
    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._count: int = 0

    def inc(self) -> None:
        with self._mu:
            self._count += 1

    def add(self, v: int) -> None:
        with self._mu:
            self._count += v

    def count(self) -> int:
        with self._mu:
            return self._count


# ── TestInt64Gauge ────────────────────────────────────────────────────────────

class TestInt64Gauge(Int64Gauge):
    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._val: int = 0

    def set(self, v: int) -> None:
        with self._mu:
            self._val = v

    def inc(self) -> None:
        with self._mu:
            self._val += 1

    def dec(self) -> None:
        with self._mu:
            self._val -= 1

    def add(self, delta: int) -> None:
        with self._mu:
            self._val += delta

    def sub(self, delta: int) -> None:
        with self._mu:
            self._val -= delta

    def value(self) -> int:
        with self._mu:
            return self._val


# ── TestFloat64Histogram ──────────────────────────────────────────────────────

class TestFloat64Histogram(Float64Histogram):
    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._count: int = 0
        self._sum: float = 0.0
        self._values: list[float] = []

    def observe(self, v: float) -> None:
        with self._mu:
            self._count += 1
            self._sum += v
            self._values.append(v)

    def count(self) -> int:
        with self._mu:
            return self._count

    def sum(self) -> float:
        with self._mu:
            return self._sum

    def values(self) -> list[float]:
        with self._mu:
            return list(self._values)


# ── Vec adapters ──────────────────────────────────────────────────────────────

class _TestInt64CounterVec(Int64CounterVec):
    def __init__(self, engine: 'TestMetrics', name: str, base: Labels) -> None:
        self._engine = engine
        self._name = name
        self._base = base

    def with_(self, labels: Labels) -> Int64Counter:
        return self._engine._get_or_create_counter(self._name, _merge_labels(self._base, labels))


class _TestInt64GaugeVec(Int64GaugeVec):
    def __init__(self, engine: 'TestMetrics', name: str, base: Labels) -> None:
        self._engine = engine
        self._name = name
        self._base = base

    def with_(self, labels: Labels) -> Int64Gauge:
        return self._engine._get_or_create_gauge(self._name, _merge_labels(self._base, labels))

    def delete(self, labels: Labels) -> None:
        pass


class _TestFloat64HistogramVec(Float64HistogramVec):
    def __init__(self, engine: 'TestMetrics', name: str, base: Labels) -> None:
        self._engine = engine
        self._name = name
        self._base = base

    def with_(self, labels: Labels) -> Float64Histogram:
        return self._engine._get_or_create_histogram(self._name, _merge_labels(self._base, labels))


# ── Noop Float64CounterVec, Float64Gauge, Int64Histogram (unused in tests) ───

class _NoopFloat64Counter(Float64Counter):
    def add(self, v: float) -> None: pass

class _NoopFloat64CounterVec(Float64CounterVec):
    def with_(self, labels: Labels) -> Float64Counter:
        return _NoopFloat64Counter()

class _NoopFloat64Gauge(Float64Gauge):
    def set(self, v: float) -> None: pass
    def inc(self) -> None: pass
    def dec(self) -> None: pass
    def add(self, delta: float) -> None: pass
    def sub(self, delta: float) -> None: pass

class _NoopFloat64GaugeVec(Float64GaugeVec):
    def with_(self, labels: Labels) -> Float64Gauge:
        return _NoopFloat64Gauge()
    def delete(self, labels: Labels) -> None: pass

class _NoopInt64Histogram(Int64Histogram):
    def observe(self, v: int) -> None: pass

class _NoopInt64HistogramVec(Int64HistogramVec):
    def with_(self, labels: Labels) -> Int64Histogram:
        return _NoopInt64Histogram()


# ── TestScope ─────────────────────────────────────────────────────────────────

class _TestScope(MetricsScope):
    def __init__(self, engine: 'TestMetrics', prefix: str, base: Labels) -> None:
        self._engine = engine
        self._prefix = prefix
        self._base = base

    def _full(self, name: str) -> str:
        return f"{self._prefix}_{name}" if name else self._prefix

    def counter(self, name: str, help: str, labels: Labels) -> Int64Counter:
        return self._engine._get_or_create_counter(self._full(name), _merge_labels(self._base, labels))

    def counter_vec(self, name: str, help: str) -> Int64CounterVec:
        return _TestInt64CounterVec(self._engine, self._full(name), self._base)

    def gauge(self, name: str, help: str, labels: Labels) -> Int64Gauge:
        return self._engine._get_or_create_gauge(self._full(name), _merge_labels(self._base, labels))

    def gauge_vec(self, name: str, help: str) -> Int64GaugeVec:
        return _TestInt64GaugeVec(self._engine, self._full(name), self._base)

    def histogram(self, name: str, help: str, labels: Labels, *buckets: float) -> Float64Histogram:
        return self._engine._get_or_create_histogram(self._full(name), _merge_labels(self._base, labels))

    def histogram_vec(self, name: str, help: str, *buckets: float) -> Float64HistogramVec:
        return _TestFloat64HistogramVec(self._engine, self._full(name), self._base)

    def observable_float64_gauge(self, name: str, help: str, fn: Callable[[], float]) -> None:
        pass


# ── TestMetrics ───────────────────────────────────────────────────────────────

class TestMetrics(MetricsEngine, Metrics):
    """Implements MetricsEngine + Metrics with in-memory storage for test assertions."""

    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._counters: dict[str, TestInt64Counter] = {}
        self._gauges: dict[str, TestInt64Gauge] = {}
        self._histograms: dict[str, TestFloat64Histogram] = {}

    def reset(self) -> None:
        """Clear all recorded values."""
        with self._mu:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    # ── MetricsEngine ────────────────────────────────────────────────────────

    @property
    def metrics(self) -> Metrics:
        return self

    @property
    def metrics_handler(self) -> MetricsHandler:
        return lambda: b""

    async def shutdown(self) -> None:
        pass

    # ── Metrics ──────────────────────────────────────────────────────────────

    def scope(self, prefix: str, labels: Labels) -> MetricsScope:
        return _TestScope(self, prefix, labels)

    # ── Test accessors ────────────────────────────────────────────────────────

    def counter(self, name: str, labels: Labels) -> TestInt64Counter:
        """Return (or create) the counter for name+labels."""
        return self._get_or_create_counter(name, labels)

    def gauge(self, name: str, labels: Labels) -> TestInt64Gauge:
        """Return (or create) the gauge for name+labels."""
        return self._get_or_create_gauge(name, labels)

    def histogram(self, name: str, labels: Labels) -> TestFloat64Histogram:
        """Return (or create) the histogram for name+labels."""
        return self._get_or_create_histogram(name, labels)

    def registered_names(self) -> list[str]:
        """Sorted unique metric names registered since last reset()."""
        with self._mu:
            names: set[str] = set()
            for key in self._counters:
                names.add(_name_from_key(key))
            for key in self._gauges:
                names.add(_name_from_key(key))
            for key in self._histograms:
                names.add(_name_from_key(key))
        return sorted(names)

    # ── Internal ─────────────────────────────────────────────────────────────

    def _get_or_create_counter(self, name: str, labels: Labels) -> TestInt64Counter:
        key = _metric_key(name, labels)
        with self._mu:
            if key not in self._counters:
                self._counters[key] = TestInt64Counter()
            return self._counters[key]

    def _get_or_create_gauge(self, name: str, labels: Labels) -> TestInt64Gauge:
        key = _metric_key(name, labels)
        with self._mu:
            if key not in self._gauges:
                self._gauges[key] = TestInt64Gauge()
            return self._gauges[key]

    def _get_or_create_histogram(self, name: str, labels: Labels) -> TestFloat64Histogram:
        key = _metric_key(name, labels)
        with self._mu:
            if key not in self._histograms:
                self._histograms[key] = TestFloat64Histogram()
            return self._histograms[key]


# ── Fluent assertion API ──────────────────────────────────────────────────────

class Expect:
    """Entry point for fluent metric assertions.

    Usage::

        Expect(engine).counter("stream_messages_total").with_("from", "X").eq(3)
        Expect(engine).gauge("service_info").with_("service", "svc").eq(1)
        Expect(engine).histogram("request_duration").with_("connector", "http").has_observations(5)
    """

    def __init__(self, engine: TestMetrics) -> None:
        self._engine = engine

    def counter(self, name: str) -> 'CounterAssertion':
        return CounterAssertion(self._engine, name, {})

    def gauge(self, name: str) -> 'GaugeAssertion':
        return GaugeAssertion(self._engine, name, {})

    def histogram(self, name: str) -> 'HistogramAssertion':
        return HistogramAssertion(self._engine, name, {})


class CounterAssertion:
    def __init__(self, engine: TestMetrics, name: str, labels: Labels) -> None:
        self._engine = engine
        self._name = name
        self._labels = labels

    def with_(self, key: str, value: str) -> 'CounterAssertion':
        new_labels = dict(self._labels)
        new_labels[key] = value
        return CounterAssertion(self._engine, self._name, new_labels)

    def eq(self, expected: int) -> None:
        got = self._engine.counter(self._name, self._labels).count()
        assert got == expected, (
            f"Counter({self._name!r}{_fmt_labels(self._labels)}): expected {expected}, got {got}"
        )

    def gt(self, minimum: int) -> None:
        got = self._engine.counter(self._name, self._labels).count()
        assert got > minimum, (
            f"Counter({self._name!r}{_fmt_labels(self._labels)}): expected > {minimum}, got {got}"
        )

    def sum(self) -> 'Int64Aggregation':
        """Sum over all series matching name and (partial) labels."""
        total = 0
        with self._engine._mu:
            for key, c in self._engine._counters.items():
                if _key_matches(key, self._name, self._labels):
                    total += c.count()
        return Int64Aggregation(total, f"Counter({self._name!r}{_fmt_labels(self._labels)}).sum()")


class GaugeAssertion:
    def __init__(self, engine: TestMetrics, name: str, labels: Labels) -> None:
        self._engine = engine
        self._name = name
        self._labels = labels

    def with_(self, key: str, value: str) -> 'GaugeAssertion':
        new_labels = dict(self._labels)
        new_labels[key] = value
        return GaugeAssertion(self._engine, self._name, new_labels)

    def eq(self, expected: int) -> None:
        got = self._engine.gauge(self._name, self._labels).value()
        assert got == expected, (
            f"Gauge({self._name!r}{_fmt_labels(self._labels)}): expected {expected}, got {got}"
        )

    def gt(self, minimum: int) -> None:
        got = self._engine.gauge(self._name, self._labels).value()
        assert got > minimum, (
            f"Gauge({self._name!r}{_fmt_labels(self._labels)}): expected > {minimum}, got {got}"
        )


class HistogramAssertion:
    def __init__(self, engine: TestMetrics, name: str, labels: Labels) -> None:
        self._engine = engine
        self._name = name
        self._labels = labels

    def with_(self, key: str, value: str) -> 'HistogramAssertion':
        new_labels = dict(self._labels)
        new_labels[key] = value
        return HistogramAssertion(self._engine, self._name, new_labels)

    def has_observations(self, expected: int) -> None:
        got = self._engine.histogram(self._name, self._labels).count()
        assert got == expected, (
            f"Histogram({self._name!r}{_fmt_labels(self._labels)}): expected {expected} observations, got {got}"
        )

    def sum_eq(self, expected: float, delta: float = 0.0) -> None:
        got = self._engine.histogram(self._name, self._labels).sum()
        assert abs(got - expected) <= delta, (
            f"Histogram({self._name!r}{_fmt_labels(self._labels)}).sum(): expected {expected}±{delta}, got {got}"
        )

    def sum_gt(self, minimum: float) -> None:
        got = self._engine.histogram(self._name, self._labels).sum()
        assert got > minimum, (
            f"Histogram({self._name!r}{_fmt_labels(self._labels)}).sum(): expected > {minimum}, got {got}"
        )


class Int64Aggregation:
    def __init__(self, value: int, desc: str) -> None:
        self._value = value
        self._desc = desc

    def eq(self, expected: int) -> None:
        assert self._value == expected, f"{self._desc}: expected {expected}, got {self._value}"

    def gt(self, minimum: int) -> None:
        assert self._value > minimum, f"{self._desc}: expected > {minimum}, got {self._value}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _labels_key(labels: Labels) -> str:
    return "\x00".join(f"{k}={v}" for k, v in sorted(labels.items())) + ("\x00" if labels else "")


def _metric_key(name: str, labels: Labels) -> str:
    return name + "\x00" + _labels_key(labels)


def _name_from_key(key: str) -> str:
    return key.split("\x00", 1)[0]


def _key_matches(storage_key: str, name: str, filter_labels: Labels) -> bool:
    prefix = name + "\x00"
    if not storage_key.startswith(prefix):
        return False
    label_part = storage_key[len(prefix):]
    return all(f"{k}={v}\x00" in label_part for k, v in filter_labels.items())


def _merge_labels(base: Labels, extra: Labels) -> Labels:
    if not extra:
        return base
    if not base:
        return extra
    merged = dict(base)
    merged.update(extra)
    return merged


def _fmt_labels(labels: Labels) -> str:
    if not labels:
        return ""
    parts = ", ".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return "{" + parts + "}"

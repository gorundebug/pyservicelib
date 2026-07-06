#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import ABC, abstractmethod
from typing import Callable

type Labels = dict[str, str]
type MetricsHandler = Callable[[], bytes]

DEFAULT_MAX_CARDINALITY = 1000


# ── Counters ──────────────────────────────────────────────────────────────────

class Int64Counter(ABC):
    @abstractmethod
    def inc(self) -> None: ...

    @abstractmethod
    def add(self, v: int) -> None: ...


class Int64CounterVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Int64Counter: ...


class Float64Counter(ABC):
    @abstractmethod
    def add(self, v: float) -> None: ...


class Float64CounterVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Float64Counter: ...


# ── Gauges ────────────────────────────────────────────────────────────────────

class Int64Gauge(ABC):
    @abstractmethod
    def set(self, v: int) -> None: ...

    @abstractmethod
    def inc(self) -> None: ...

    @abstractmethod
    def dec(self) -> None: ...

    @abstractmethod
    def add(self, delta: int) -> None: ...

    @abstractmethod
    def sub(self, delta: int) -> None: ...


class Int64GaugeVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Int64Gauge: ...

    @abstractmethod
    def delete(self, labels: Labels) -> None: ...


class Float64Gauge(ABC):
    @abstractmethod
    def set(self, v: float) -> None: ...

    @abstractmethod
    def inc(self) -> None: ...

    @abstractmethod
    def dec(self) -> None: ...

    @abstractmethod
    def add(self, delta: float) -> None: ...

    @abstractmethod
    def sub(self, delta: float) -> None: ...


class Float64GaugeVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Float64Gauge: ...

    @abstractmethod
    def delete(self, labels: Labels) -> None: ...


# ── Histograms ────────────────────────────────────────────────────────────────

class Float64Histogram(ABC):
    @abstractmethod
    def observe(self, v: float) -> None: ...


class Float64HistogramVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Float64Histogram: ...


class Int64Histogram(ABC):
    @abstractmethod
    def observe(self, v: int) -> None: ...


class Int64HistogramVec(ABC):
    @abstractmethod
    def with_(self, labels: Labels) -> Int64Histogram: ...


# ── Noop implementations ──────────────────────────────────────────────────────

class _NoopInt64Counter(Int64Counter):
    def inc(self) -> None: pass
    def add(self, v: int) -> None: pass


class _NoopFloat64Counter(Float64Counter):
    def add(self, v: float) -> None: pass


class _NoopInt64Gauge(Int64Gauge):
    def set(self, v: int) -> None: pass
    def inc(self) -> None: pass
    def dec(self) -> None: pass
    def add(self, delta: int) -> None: pass
    def sub(self, delta: int) -> None: pass


class _NoopFloat64Gauge(Float64Gauge):
    def set(self, v: float) -> None: pass
    def inc(self) -> None: pass
    def dec(self) -> None: pass
    def add(self, delta: float) -> None: pass
    def sub(self, delta: float) -> None: pass


class _NoopFloat64Histogram(Float64Histogram):
    def observe(self, v: float) -> None: pass


class _NoopInt64Histogram(Int64Histogram):
    def observe(self, v: int) -> None: pass


NOOP_INT64_COUNTER: Int64Counter = _NoopInt64Counter()
NOOP_FLOAT64_COUNTER: Float64Counter = _NoopFloat64Counter()
NOOP_INT64_GAUGE: Int64Gauge = _NoopInt64Gauge()
NOOP_FLOAT64_GAUGE: Float64Gauge = _NoopFloat64Gauge()
NOOP_FLOAT64_HISTOGRAM: Float64Histogram = _NoopFloat64Histogram()
NOOP_INT64_HISTOGRAM: Int64Histogram = _NoopInt64Histogram()


# ── MetricsScope ──────────────────────────────────────────────────────────────

class MetricsScope(ABC):
    """Factory bound to a fixed name prefix and base labels."""

    @abstractmethod
    def counter(self, name: str, help: str, labels: Labels) -> Int64Counter: ...

    @abstractmethod
    def counter_vec(self, name: str, help: str) -> Int64CounterVec: ...

    @abstractmethod
    def gauge(self, name: str, help: str, labels: Labels) -> Int64Gauge: ...

    @abstractmethod
    def gauge_vec(self, name: str, help: str) -> Int64GaugeVec: ...

    @abstractmethod
    def histogram(self, name: str, help: str, labels: Labels, *buckets: float) -> Float64Histogram: ...

    @abstractmethod
    def histogram_vec(self, name: str, help: str, *buckets: float) -> Float64HistogramVec: ...

    @abstractmethod
    def observable_float64_gauge(self, name: str, help: str, fn: Callable[[], float]) -> None: ...


# ── Metrics ───────────────────────────────────────────────────────────────────

class Metrics(ABC):
    @abstractmethod
    def scope(self, prefix: str, labels: Labels) -> MetricsScope: ...


# ── MetricsEngine ─────────────────────────────────────────────────────────────

class MetricsEngine(ABC):
    @property
    @abstractmethod
    def metrics(self) -> Metrics: ...

    @property
    @abstractmethod
    def metrics_handler(self) -> MetricsHandler: ...

    async def shutdown(self) -> None:
        pass

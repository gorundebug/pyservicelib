#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import asyncio
from typing import Any, Optional

import pyservicelib.runtime.environment.metrics as metrics
import prometheus_client

from pyservicelib.runtime.environment.metrics import MetricsHandler, Metrics


class Counter(metrics.Counter):
    _counter: prometheus_client.Counter

    def __init__(self, counter: prometheus_client.Counter):
        self._counter = counter

    def inc(self, amount: float = 1) -> None:
        self._counter.inc(amount=amount)

    def labels(self, *label_values: Any, **label_kwargs: Any) -> "Counter":
        return self.__class__(self._counter.labels(*label_values, **label_kwargs))


class Summary(metrics.Summary):

    _summary: prometheus_client.Summary

    def __init__(self, summary: prometheus_client.Summary):
        self._summary = summary

    def observe(self, amount: float) -> None:
        self._summary.observe(amount=amount)

    def labels(self, *label_values: Any, **label_kwargs: Any) -> "Summary":
        return self.__class__(self._summary.labels(*label_values, **label_kwargs))


class Gauge(metrics.Gauge):

    _gauge: prometheus_client.Gauge

    def __init__(self, gauge: prometheus_client.Gauge):
        self._gauge = gauge

    def inc(self, amount: float = 1) -> None:
        self._gauge.inc(amount=amount)

    def dec(self, amount: float = 1) -> None:
        self._gauge.dec(amount=amount)

    def set(self, value: float) -> None:
        self._gauge.set(value=value)

    def set_to_current_time(self) -> None:
        self._gauge.set_to_current_time()

    def labels(self, *label_values: Any, **label_kwargs: Any) -> "Gauge":
        return self.__class__(self._gauge.labels(*label_values, **label_kwargs))


class Histogram(metrics.Histogram):

    _histogram: prometheus_client.Histogram

    def __init__(self, histogram: prometheus_client.Histogram):
        self._histogram = histogram

    def observe(self, amount: float):
        self._histogram.observe(amount=amount)

    def labels(self, *label_values: Any, **label_kwargs: Any) -> "Histogram":
        return self.__class__(self._histogram.labels(*label_values, **label_kwargs))


class CounterVec(metrics.CounterVec):
    _counter: Counter

    def __init__(self, opts: metrics.CounterOpts):
        self._counter = Counter(prometheus_client.Counter(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Counter:
        return self._counter.labels(*label_values, **label_kwargs)


class SummaryVec(metrics.SummaryVec):
    _summary: Summary

    def __init__(self, opts: metrics.SummaryOpts):
        self._summary = Summary(prometheus_client.Summary(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Summary:
        return self._summary.labels(*label_values, **label_kwargs)


class GaugeVec(metrics.GaugeVec):
    _gauge: Gauge

    def __init__(self, opts: metrics.GaugeOpts):
        self._gauge = Gauge(prometheus_client.Gauge(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Gauge:
        return self._gauge.labels(*label_values, **label_kwargs)


class HistogramVec(metrics.HistogramVec):
    _histogram: Histogram

    def __init__(self, opts: metrics.HistogramOpts):
        self._histogram = Histogram(prometheus_client.Histogram(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            buckets=opts.buckets
        ))

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Histogram:
        return self._histogram.labels(*label_values, **label_kwargs)


class PrometheusMetrics(metrics.Metrics):

    def __init__(self):
        pass

    def histogram_vec(self, opts: metrics.HistogramOpts) -> metrics.HistogramVec:
        return HistogramVec(opts)

    def summary_vec(self, opts: metrics.SummaryOpts) -> metrics.SummaryVec:
        return SummaryVec(opts)

    def gauge_vec(self, opts: metrics.GaugeOpts) -> metrics.GaugeVec:
        return GaugeVec(opts)

    def counter_vec(self, opts: metrics.CounterOpts) -> metrics.CounterVec:
        return CounterVec(opts)

    def histogram(self, opts: metrics.HistogramOpts) -> metrics.Histogram:
        return Histogram(prometheus_client.Histogram(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            buckets=opts.buckets
        ))

    def summary(self, opts: metrics.SummaryOpts) -> metrics.Summary:
        return Summary(prometheus_client.Summary(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))

    def gauge(self, opts: metrics.GaugeOpts) -> metrics.Gauge:
        return Gauge(prometheus_client.Gauge(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))

    def counter(self, opts: metrics.CounterOpts) -> metrics.Counter:
        return Counter(prometheus_client.Counter(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        ))


class PrometheusMetricsEngine(metrics.MetricsEngine):

    _engine: Optional["PrometheusMetricsEngine"] = None
    _lock: asyncio.Lock = asyncio.Lock()
    _metrics: PrometheusMetrics

    def __init__(self):
        self._metrics = PrometheusMetrics()

    @classmethod
    async def engine(cls):
        if cls._engine is None:
            async with cls._lock:
                if cls._engine is None:
                    cls._engine = PrometheusMetricsEngine()
        return cls._engine

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def metrics_handler(self) -> MetricsHandler:
        return lambda: prometheus_client.generate_latest()


#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import asyncio
from typing import Any, Optional, Sequence
import prometheus_client

from ...environment import metrics as metrics
from ...environment.metrics import MetricsHandler

class Counter(metrics.Counter):
    __slots__ = ['_counter']

    _reserved_labels: Sequence[str] = ()
    _counter: prometheus_client.Counter

    def __init__(self, counter: prometheus_client.Counter):
        self._counter = counter

    @classmethod
    def reserved_labels(cls) -> Sequence[str]:
        return cls._reserved_labels

    def inc(self, amount: float = 1) -> None:
        self._counter.inc(amount=amount)


class Summary(metrics.Summary):
    __slots__ = ['_summary']

    _reserved_labels: Sequence[str] = ['quantile']
    _summary: prometheus_client.Summary

    def __init__(self, summary: prometheus_client.Summary):
        self._summary = summary

    @classmethod
    def reserved_labels(cls) -> Sequence[str]:
        return cls._reserved_labels

    def observe(self, amount: float) -> None:
        self._summary.observe(amount=amount)


class Gauge(metrics.Gauge):

    __slots__ = ['_gauge']

    _reserved_labels: Sequence[str] = ()
    _gauge: prometheus_client.Gauge

    def __init__(self, gauge: prometheus_client.Gauge):
        self._gauge = gauge

    @classmethod
    def reserved_labels(cls) -> Sequence[str]:
        return cls._reserved_labels

    def inc(self, amount: float = 1) -> None:
        self._gauge.inc(amount=amount)

    def dec(self, amount: float = 1) -> None:
        self._gauge.dec(amount=amount)

    def set(self, value: float) -> None:
        self._gauge.set(value=value)

    def set_to_current_time(self) -> None:
        self._gauge.set_to_current_time()


class Histogram(metrics.Histogram):
    __slots__ = ['_histogram']

    _reserved_labels: Sequence[str] = ['le']
    _histogram: prometheus_client.Histogram

    def __init__(self, histogram: prometheus_client.Histogram):
        self._histogram = histogram

    @classmethod
    def reserved_labels(cls) -> Sequence[str]:
        return cls._reserved_labels

    def observe(self, amount: float):
        self._histogram.observe(amount=amount)


class CounterVec(metrics.CounterVec):
    __slots__ = ['_counter', '_label_names', '_const_label_names', '_const_values', '_metrics']

    _counter: prometheus_client.Counter
    _label_names: metrics.Labels
    _const_label_names: metrics.Labels
    _const_values: metrics.LabelValues
    _metrics: dict[Sequence[str], Counter]

    def __init__(self, opts: metrics.CounterOpts, label_names: metrics.Labels):
        if opts.const_labels is not None:
            self._const_label_names = tuple(opts.const_labels.keys())
            self._const_values = tuple(opts.const_labels.values())
        else:
            self._const_label_names = ()
            self._const_values = ()

        self._label_names = tuple(label_names)
        self._metrics = {}

        self._counter = prometheus_client.Counter(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=tuple(self._const_label_names) + tuple(self._label_names),
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Counter:
        if label_values and label_kwargs:
            raise ValueError("Can't pass both *args and **kwargs")

        if label_kwargs:
            if sorted(label_kwargs) != sorted(self._label_names):
                raise ValueError('Incorrect label names')
            all_label_values = tuple(self._const_values) + tuple(str(label_kwargs[l]) for l in self._label_names)
        else:
            if len(label_values) != len(self._label_names):
                raise ValueError('Incorrect label count')
            all_label_values = tuple(self._const_values) + tuple(str(l) for l in label_values)

        if all_label_values not in self._metrics:
            self._metrics[all_label_values] = Counter(self._counter.labels(*all_label_values))

        return self._metrics[all_label_values]


class SummaryVec(metrics.SummaryVec):
    __slots__ = ['_summary', '_label_names', '_const_label_names', '_const_values', '_metrics']

    _summary: prometheus_client.Summary
    _label_names: metrics.Labels
    _const_label_names: metrics.Labels
    _const_values: metrics.LabelValues
    _metrics: dict[Sequence[str], Summary]

    def __init__(self, opts: metrics.SummaryOpts, label_names: metrics.Labels):
        if opts.const_labels is not None:
            self._const_label_names = tuple(opts.const_labels.keys())
            self._const_values = tuple(opts.const_labels.values())
        else:
            self._const_label_names = ()
            self._const_values = ()

        self._label_names = tuple(label_names)
        self._metrics = {}

        self._counter = prometheus_client.Summary(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=tuple(self._const_label_names) + tuple(self._label_names),
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Summary:
        if label_values and label_kwargs:
            raise ValueError("Can't pass both *args and **kwargs")

        if label_kwargs:
            if sorted(label_kwargs) != sorted(self._label_names):
                raise ValueError('Incorrect label names')
            all_label_values = tuple(self._const_values) + tuple(str(label_kwargs[l]) for l in self._label_names)
        else:
            if len(label_values) != len(self._label_names):
                raise ValueError('Incorrect label count')
            all_label_values = tuple(self._const_values) + tuple(str(l) for l in label_values)

        if all_label_values not in self._metrics:
            self._metrics[all_label_values] = Summary(self._counter.labels(*all_label_values))

        return self._metrics[all_label_values]


class GaugeVec(metrics.GaugeVec):
    __slots__ = ['_gauge', '_label_names', '_const_label_names', '_const_values', '_metrics']

    _gauge: prometheus_client.Gauge
    _label_names: metrics.Labels
    _const_label_names: metrics.Labels
    _const_values: metrics.LabelValues
    _metrics: dict[Sequence[str], Gauge]

    def __init__(self, opts: metrics.GaugeOpts, label_names: metrics.Labels):
        if opts.const_labels is not None:
            self._const_label_names = tuple(opts.const_labels.keys())
            self._const_values = tuple(opts.const_labels.values())
        else:
            self._const_label_names = ()
            self._const_values = ()

        self._label_names = tuple(label_names)
        self._metrics = {}

        self._counter = prometheus_client.Gauge(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=tuple(self._const_label_names) + tuple(self._label_names),
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Gauge:
        if label_values and label_kwargs:
            raise ValueError("Can't pass both *args and **kwargs")

        if label_kwargs:
            if sorted(label_kwargs) != sorted(self._label_names):
                raise ValueError('Incorrect label names')
            all_label_values = tuple(self._const_values) + tuple(str(label_kwargs[l]) for l in self._label_names)
        else:
            if len(label_values) != len(self._label_names):
                raise ValueError('Incorrect label count')
            all_label_values = tuple(self._const_values) + tuple(str(l) for l in label_values)

        if all_label_values not in self._metrics:
            self._metrics[all_label_values] = Gauge(self._counter.labels(*all_label_values))

        return self._metrics[all_label_values]


class HistogramVec(metrics.HistogramVec):
    __slots__ = ['_histogram', '_label_names', '_const_label_names', '_const_values', '_metrics']

    _histogram: prometheus_client.Histogram
    _label_names: metrics.Labels
    _const_label_names: metrics.Labels
    _const_values: metrics.LabelValues
    _metrics: dict[Sequence[str], Histogram]

    def __init__(self, opts: metrics.HistogramOpts, label_names: metrics.Labels):
        if opts.const_labels is not None:
            self._const_label_names = tuple(opts.const_labels.keys())
            self._const_values = tuple(opts.const_labels.values())
        else:
            self._const_label_names = ()
            self._const_values = ()

        self._label_names = tuple(label_names)
        self._metrics = {}

        self._counter = prometheus_client.Histogram(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=tuple(self._const_label_names) + tuple(self._label_names),
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            buckets=opts.buckets
        )

    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> metrics.Histogram:
        if label_values and label_kwargs:
            raise ValueError("Can't pass both *args and **kwargs")

        if label_kwargs:
            if sorted(label_kwargs) != sorted(self._label_names):
                raise ValueError('Incorrect label names')
            all_label_values = tuple(self._const_values) + tuple(str(label_kwargs[l]) for l in self._label_names)
        else:
            if len(label_values) != len(self._label_names):
                raise ValueError('Incorrect label count')
            all_label_values = tuple(self._const_values) + tuple(str(l) for l in label_values)

        if all_label_values not in self._metrics:
            self._metrics[all_label_values] = Histogram(self._counter.labels(*all_label_values))

        return self._metrics[all_label_values]


class PrometheusMetrics(metrics.Metrics):

    def __init__(self):
        pass

    def histogram_vec(self, opts: metrics.HistogramOpts, label_names: metrics.Labels) -> metrics.HistogramVec:
        return HistogramVec(opts, label_names)

    def summary_vec(self, opts: metrics.SummaryOpts, label_names: metrics.Labels) -> metrics.SummaryVec:
        return SummaryVec(opts, label_names)

    def gauge_vec(self, opts: metrics.GaugeOpts, label_names: metrics.Labels) -> metrics.GaugeVec:
        return GaugeVec(opts, label_names)

    def counter_vec(self, opts: metrics.CounterOpts, label_names: metrics.Labels) -> metrics.CounterVec:
        return CounterVec(opts, label_names)

    def histogram(self, opts: metrics.HistogramOpts) -> metrics.Histogram:

        if opts.const_labels is not None:
            label_names = tuple(*opts.const_labels.keys())
            label_values = tuple(*opts.const_labels.values())
        else:
            label_names = ()
            label_values = ()

        return Histogram(prometheus_client.Histogram(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            buckets=opts.buckets,
            _labelvalues=label_values
        ))

    def summary(self, opts: metrics.SummaryOpts) -> metrics.Summary:
        if opts.const_labels is not None:
            label_names = tuple(*opts.const_labels.keys())
            label_values = tuple(*opts.const_labels.values())
        else:
            label_names = ()
            label_values = ()

        return Summary(prometheus_client.Summary(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            _labelvalues=label_values
        ))

    def gauge(self, opts: metrics.GaugeOpts) -> metrics.Gauge:
        if opts.const_labels is not None:
            label_names = tuple(*opts.const_labels.keys())
            label_values = tuple(*opts.const_labels.values())
        else:
            label_names = ()
            label_values = ()

        return Gauge(prometheus_client.Gauge(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            _labelvalues=label_values
        ))

    def counter(self, opts: metrics.CounterOpts) -> metrics.Counter:
        if opts.const_labels is not None:
            label_names = tuple(opts.const_labels.keys())
            label_values = tuple(opts.const_labels.values())
        else:
            label_names = ()
            label_values = ()

        return Counter(prometheus_client.Counter(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            _labelvalues=label_values
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
    def metrics(self) -> metrics.Metrics:
        return self._metrics

    @property
    def metrics_handler(self) -> MetricsHandler:
        return lambda: prometheus_client.generate_latest()


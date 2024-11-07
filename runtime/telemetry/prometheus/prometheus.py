#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

import pyservicelib.runtime.environment.metrics as metrics
import prometheus_client


class Counter(metrics.Counter):
    _counter: prometheus_client.Counter

    def __init__(self, opts: metrics.CounterOpts):
        self._counter = prometheus_client.Counter(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def inc(self, amount: float = 1) -> None:
        self._counter.inc(amount=amount)


class Summary(metrics.Summary):

    _summary: prometheus_client.Summary

    def __init__(self, opts: metrics.SummaryOpts):
        self._summary = prometheus_client.Summary(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def observe(self, amount: float) -> None:
        self._summary.observe(amount=amount)


class Gauge(metrics.Gauge):

    _gauge: prometheus_client.Gauge

    def __init__(self, opts: metrics.GaugeOpts):
        self._gauge = prometheus_client.Gauge(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem
        )

    def inc(self, amount: float = 1) -> None:
        self._gauge.inc(amount=amount)

    def dec(self, amount: float = 1) -> None:
        self._gauge.dec(amount=amount)

    def set(self, value: float) -> None:
        self._gauge.set(value=value)

    def set_to_current_time(self) -> None:
        self._gauge.set_to_current_time()


class Histogram(metrics.Histogram):

    _histogram: prometheus_client.Histogram

    def __init__(self, opts: metrics.HistogramOpts):
        self._histogram = prometheus_client.Histogram(
            name=opts.name,
            documentation=opts.documentation,
            labelnames=opts.label_names,
            namespace=opts.namespace,
            subsystem=opts.subsystem,
            buckets=opts.buckets
        )

    def observe(self, amount: float):
        self._histogram.observe(amount=amount)


class Metrics(metrics.Metrics):

    def histogram_vec(self, opts: metrics.HistogramOpts) -> metrics.HistogramVec:
        pass

    def summary_vec(self, opts: metrics.SummaryOpts) -> metrics.SummaryVec:
        pass

    def gauge_vec(self, opts: metrics.GaugeOpts) -> metrics.GaugeVec:
        pass

    def counter_vec(self, opts: metrics.CounterOpts) -> metrics.CounterVec:
        pass

    def histogram(self, opts: metrics.HistogramOpts) -> metrics.Histogram:
        return Histogram(opts)

    def summary(self, opts: metrics.SummaryOpts) -> metrics.Summary:
        return Summary(opts)

    def gauge(self, opts: metrics.GaugeOpts) -> metrics.Gauge:
        return Gauge(opts)

    def counter(self, opts: metrics.CounterOpts) -> metrics.Counter:
        return Counter(opts)
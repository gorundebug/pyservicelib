#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Callable, Iterable, Sequence, Union, Any
from abc import ABC, abstractmethod

MetricsHandler = Callable[[], bytes]

class Counter(ABC):

    @abstractmethod
    def inc(self, amount: float = 1) -> None:
        pass


class CounterVec(ABC):

    @abstractmethod
    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> Counter:
        pass


class Gauge(ABC):

    @abstractmethod
    def inc(self, amount: float = 1) -> None:
        pass

    @abstractmethod
    def dec(self, amount: float = 1) -> None:
        pass

    @abstractmethod
    def set(self, value: float) -> None:
        pass

    @abstractmethod
    def set_to_current_time(self) -> None:
        pass


class GaugeVec(ABC):

    @abstractmethod
    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> Gauge:
        pass


class Histogram(ABC):
    @abstractmethod
    def observe(self, amount: float):
        pass


class HistogramVec(ABC):

    @abstractmethod
    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> Gauge:
        pass


class Summary(ABC):

    @abstractmethod
    def observe(self, amount: float) -> None:
        pass


class SummaryVec(ABC):

    @abstractmethod
    def with_label_values(self, *label_values: Any, **label_kwargs: Any) -> Summary:
        pass


type Labels = Iterable[str]

class Opts:

    def __init__(self, name: str,
                 documentation: str,
                 label_names: Labels = (),
                 namespace: str = '',
                 subsystem: str = ''):
        self.name: str = name
        self.documentation: str = documentation
        self.namespace: str = namespace
        self.subsystem: str = subsystem
        self.label_names: Labels = label_names


class CounterOpts(Opts):
    pass


class SummaryOpts(Opts):
    pass


class GaugeOpts(Opts):
    pass


class HistogramOpts(Opts):
    DEFAULT_BUCKETS = (.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, float("inf"))

    def __init__(self, name: str,
                 documentation: str,
                 label_names: Labels = (),
                 namespace: str = '',
                 subsystem: str = '',
                 buckets: Sequence[Union[float, str]] = DEFAULT_BUCKETS):
        super().__init__(name=name,
                         documentation=documentation,
                         label_names=label_names,
                         namespace=namespace,
                         subsystem=subsystem)
        self.buckets = buckets


class Metrics(ABC):

    @abstractmethod
    def counter(self, opts: CounterOpts) -> Counter:
        pass

    @abstractmethod
    def gauge(self, opts: GaugeOpts) -> Gauge:
        pass

    @abstractmethod
    def summary(self, opts: SummaryOpts) -> Summary:
        pass

    @abstractmethod
    def histogram(self, opts: HistogramOpts) -> Histogram:
        pass

    @abstractmethod
    def counter_vec(self, opts: CounterOpts) -> CounterVec:
        pass

    @abstractmethod
    def gauge_vec(self, opts: GaugeOpts) -> GaugeVec:
        pass

    @abstractmethod
    def summary_vec(self, opts: SummaryOpts) -> SummaryVec:
        pass

    @abstractmethod
    def histogram_vec(self, opts: HistogramOpts) -> HistogramVec:
        pass


class MetricsEngine(ABC):
    @property
    @abstractmethod
    def metrics(self) -> Metrics:
        pass

    @property
    @abstractmethod
    def metrics_handler(self) -> MetricsHandler:
        pass
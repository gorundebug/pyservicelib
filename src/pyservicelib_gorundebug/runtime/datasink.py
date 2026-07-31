#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import time
from abc import ABC
from typing import Optional, Iterable, cast, Any

from .common import ServiceExecutionEnvironment, Consumer
from .common import DataConnector, DataSink
from .common import SinkEndpoint, OutputEndpointConsumer, TypedSinkStream
from .config import DataConnectorConfig, EndpointConfig, data_connector_protocol
from .environment.metrics import Int64Counter, Int64Gauge, Float64Histogram
from .environment.log.log import str_field, err_field
from .transportmetrics import TransportRequest, TransportRequestMetrics


class OutputDataSink(DataSink, ABC):
    _id: int
    _environment: ServiceExecutionEnvironment
    _endpoints: dict[int, SinkEndpoint]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        self._id = connector_id
        self._environment = env
        self._endpoints = {}

    @property
    def data_connector(self) -> DataConnectorConfig:
        return self._environment.config.get_data_connector_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._environment

    def add_endpoint(self, endpoint: SinkEndpoint) -> None:
        self._endpoints[endpoint.id] = endpoint

    def get_endpoint(self, id_endpoint: int) -> Optional[SinkEndpoint]:
        return self._endpoints.get(id_endpoint)

    @property
    def endpoints(self) ->  Iterable[SinkEndpoint]:
        return self._endpoints.values()

    @property
    def name(self) -> str:
        return self.data_connector.name

    @property
    def id(self) -> int:
        return self._id


class DataSinkEndpoint(SinkEndpoint):
    _id: int
    _data_sink: DataSink
    _endpoint_consumers: list[OutputEndpointConsumer]
    _begin_request_failed_counter: Int64Counter
    _request_errors_counter: Int64Counter
    _messages_total: Int64Counter
    _request_duration: Float64Histogram
    _active_requests: Int64Gauge
    _late_result_counter: Int64Counter
    _transport_metrics: Optional[TransportRequestMetrics]
    _transport_requests: dict[float, TransportRequest]

    def __init__(self, data_sink: DataSink, id_endpoint: int):
        self._id = id_endpoint
        self._data_sink = data_sink
        self._endpoint_consumers = []
        self._transport_requests = {}

        endpoint_config = data_sink.environment.config.get_endpoint_config_by_id(id_endpoint)
        endpoint_name = endpoint_config.name
        connector_name = data_sink.name

        labels = {
            "connector": connector_name,
            "endpoint": endpoint_name,
        }
        protocol = data_connector_protocol(data_sink.data_connector.type)
        if protocol is not None:
            labels["protocol"] = protocol
        scope = data_sink.environment.metrics.scope("datasink_endpoint", labels)
        self._begin_request_failed_counter = scope.counter(
            "events_total", "Total number of events in data sink endpoint",
            {"event": "begin_request_failed"},
        )
        self._request_errors_counter = scope.counter(
            "events_total", "Total number of events in data sink endpoint",
            {"event": "request_error"},
        )
        self._late_result_counter = scope.counter(
            "events_total", "Total number of events in data sink endpoint",
            {"event": "late_result"},
        )
        self._messages_total = scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data sink endpoint",
            {},
        )
        self._request_duration = scope.histogram(
            "request_duration_seconds",
            "Request duration in seconds for data sink endpoint",
            {},
        )
        self._active_requests = scope.gauge(
            "active_requests", "Number of active requests in data sink endpoint", {},
        )

        path = getattr(endpoint_config, "path", None)
        method_name = getattr(endpoint_config, "method_name", None)
        if path:
            self._transport_metrics = TransportRequestMetrics.http_client(
                data_sink.environment.metrics,
                method=getattr(endpoint_config, "method", None) or "UNKNOWN",
                route=path,
                server_address=connector_name,
            )
        elif method_name:
            self._transport_metrics = TransportRequestMetrics.grpc_client(
                data_sink.environment.metrics,
                method=f"{connector_name}/{method_name}",
            )
        else:
            self._transport_metrics = None

    @property
    def config(self) -> EndpointConfig:
        return self.environment.config.get_endpoint_config_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._data_sink.environment

    @property
    def datasink(self) -> DataSink:
        return self._data_sink

    def add_endpoint_consumer(self, consumer: OutputEndpointConsumer) -> None:
        self._endpoint_consumers.append(consumer)

    @property
    def endpoint_consumers(self) -> Iterable[OutputEndpointConsumer]:
        return self._endpoint_consumers

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def id(self) -> int:
        return self._id

    @property
    def data_connector(self) -> DataConnector:
        return self._data_sink

    def on_begin_request_failed(self, err: Exception) -> None:
        self._data_sink.environment.log.error(
            "begin_request failed",
            str_field("endpoint", self.name),
            err_field(err),
        )
        self._begin_request_failed_counter.inc()

    def on_late_result(self, stream_id: str) -> None:
        self._data_sink.environment.log.warn(
            "late result received",
            str_field("endpoint", self.name),
            str_field("stream_id", stream_id),
        )
        self._late_result_counter.inc()

    def on_request_start(self) -> float:
        self._active_requests.inc()
        started_at = time.monotonic()
        if self._transport_metrics is not None:
            self._transport_requests[started_at] = self._transport_metrics.start()
        return started_at

    def on_request_end(
        self,
        start_time: float,
        err: Optional[Exception],
        status: Optional[str] = None,
        request_body_size: Optional[int] = None,
        response_body_size: Optional[int] = None,
    ) -> None:
        self._active_requests.dec()
        self._request_duration.observe(time.monotonic() - start_time)
        transport_request = self._transport_requests.pop(start_time, None)
        if self._transport_metrics is not None and transport_request is not None:
            self._transport_metrics.finish(
                transport_request,
                err,
                status,
                request_body_size,
                response_body_size,
            )
        if err is None:
            self._messages_total.inc()
        else:
            self._request_errors_counter.inc()


class DataSinkEndpointConsumer[T, E](OutputEndpointConsumer):
    """Data holder for sink endpoint consumers. Derived classes implement consume()."""
    _sink_stream: TypedSinkStream[T, E]
    _endpoint: SinkEndpoint

    def __init__(self, endpoint: SinkEndpoint, sink_stream: TypedSinkStream[T, E]):
        self._endpoint = endpoint
        self._sink_stream = sink_stream

    @property
    def stream(self) -> TypedSinkStream[T, E]:
        return self._sink_stream

    @property
    def endpoint(self) -> SinkEndpoint:
        return self._endpoint

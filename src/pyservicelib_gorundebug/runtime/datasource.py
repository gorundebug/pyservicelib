#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import time
from abc import ABC
from typing import Optional, Iterable, cast, Any

from .common import ServiceExecutionEnvironment, Consumer, TypedInputStream
from .common import DataSource, InputEndpoint, DataConnector
from .common import InputEndpointConsumer, StreamContext, CollectFunc
from .config import DataConnectorConfig, EndpointConfig
from .environment.metrics import Int64Counter, Int64Gauge, Float64Histogram
from .environment.log.log import str_field, err_field
from .transportmetrics import TransportRequest, TransportRequestMetrics


class InputDataSource(DataSource, ABC):
    _id: int
    _environment: ServiceExecutionEnvironment
    _endpoints: dict[int, InputEndpoint]

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

    def add_endpoint(self, endpoint: InputEndpoint) -> None:
        self._endpoints[endpoint.id] = endpoint

    def get_endpoint(self, id_endpoint: int) -> Optional[InputEndpoint]:
        return self._endpoints.get(id_endpoint)

    @property
    def endpoints(self) ->  Iterable[InputEndpoint]:
        return self._endpoints.values()

    @property
    def name(self) -> str:
        return self.data_connector.name

    @property
    def id(self) -> int:
        return self._id


class DataSourceEndpoint(InputEndpoint):
    _id: int
    _datasource: DataSource
    _endpoint_consumers: list[InputEndpointConsumer]
    _missing_stream_id_counter: Int64Counter
    _late_result_counter: Int64Counter
    _unknown_message_id_counter: Int64Counter
    _duplicate_message_id_counter: Int64Counter
    _request_errors_counter: Int64Counter
    _begin_request_failed_counter: Int64Counter
    _invalid_http_method_counter: Int64Counter
    _messages_total: Int64Counter
    _request_duration: Float64Histogram
    _active_requests: Int64Gauge
    _pending_requests: Int64Gauge
    _pending_start_times: dict[str, float]
    _transport_metrics: Optional[TransportRequestMetrics]
    _transport_requests: dict[float, TransportRequest]

    def __init__(self, datasource: DataSource, id_endpoint: int):
        self._id = id_endpoint
        self._datasource = datasource
        self._endpoint_consumers = []
        self._pending_start_times = {}
        self._transport_requests = {}

        endpoint_config = datasource.environment.config.get_endpoint_config_by_id(id_endpoint)
        endpoint_name = endpoint_config.name
        connector_name = datasource.name

        scope = datasource.environment.metrics.scope("datasource_endpoint", {
            "connector": connector_name,
            "endpoint": endpoint_name,
        })
        self._missing_stream_id_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "missing_stream_id"},
        )
        self._late_result_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "late_result"},
        )
        self._unknown_message_id_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "unknown_message_id"},
        )
        self._duplicate_message_id_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "duplicate_message_id"},
        )
        self._request_errors_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "request_error"},
        )
        self._begin_request_failed_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "begin_request_failed"},
        )
        self._invalid_http_method_counter = scope.counter(
            "events_total", "Total number of events in data source endpoint",
            {"event": "invalid_http_method"},
        )
        self._messages_total = scope.counter(
            "messages_total",
            "Total number of successfully processed messages in data source endpoint",
            {},
        )
        self._request_duration = scope.histogram(
            "request_duration_seconds",
            "Request duration in seconds for data source endpoint",
            {},
        )
        self._active_requests = scope.gauge(
            "active_requests", "Number of active requests in data source endpoint", {},
        )
        self._pending_requests = scope.gauge(
            "pending_requests", "Number of requests awaiting a pipeline result", {},
        )

        def _oldest_pending_age() -> float:
            if not self._pending_start_times:
                return 0.0
            now = time.monotonic()
            return max(now - t for t in self._pending_start_times.values())

        scope.observable_float64_gauge(
            "pending_oldest_age_seconds",
            "Age in seconds of the oldest pending request awaiting a pipeline result",
            _oldest_pending_age,
        )

        path = getattr(endpoint_config, "path", None)
        method_name = getattr(endpoint_config, "method_name", None)
        if path:
            service = datasource.environment.service_config
            self._transport_metrics = TransportRequestMetrics.http_server(
                datasource.environment.metrics,
                method=getattr(endpoint_config, "method", None) or "UNKNOWN",
                route=path,
                host=service.http_host,
                port=service.http_port,
            )
        elif method_name:
            self._transport_metrics = TransportRequestMetrics.grpc_server(
                datasource.environment.metrics,
                method=f"{connector_name}/{method_name}",
            )
        else:
            self._transport_metrics = None

    @property
    def config(self) -> EndpointConfig:
        return self.environment.config.get_endpoint_config_by_id(self._id)

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._datasource.environment

    @property
    def datasource(self) -> DataSource:
        return self._datasource

    def add_endpoint_consumer(self, consumer: InputEndpointConsumer) -> None:
        self._endpoint_consumers.append(consumer)

    @property
    def endpoint_consumers(self) -> Iterable[InputEndpointConsumer]:
        return self._endpoint_consumers

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def id(self) -> int:
        return self._id

    @property
    def data_connector(self) -> DataConnector:
        return self._datasource

    def on_missing_stream_id(self) -> None:
        self._datasource.environment.log.error(
            "consume_result called without stream_id",
            str_field("endpoint", self.name),
        )
        self._missing_stream_id_counter.inc()

    def on_late_result(self, stream_id: str) -> None:
        self._datasource.environment.log.warn(
            "consume_result: session not found in pending",
            str_field("endpoint", self.name),
            str_field("session_id", stream_id),
        )
        self._late_result_counter.inc()

    def on_unknown_message_id(self, stream_id: str, message_id: str) -> None:
        self._datasource.environment.log.warn(
            "consume_result: unknown message ID",
            str_field("endpoint", self.name),
            str_field("message_id", message_id),
            str_field("session_id", stream_id),
        )
        self._unknown_message_id_counter.inc()

    def on_duplicate_message_id(self, stream_id: str, message_id: str) -> None:
        self._datasource.environment.log.warn(
            "consume_result: duplicate message ID",
            str_field("endpoint", self.name),
            str_field("message_id", message_id),
            str_field("session_id", stream_id),
        )
        self._duplicate_message_id_counter.inc()

    def on_pending_add(self, stream_id: str) -> None:
        self._pending_requests.inc()
        self._pending_start_times[stream_id] = time.monotonic()

    def on_pending_remove(self, stream_id: str) -> None:
        self._pending_requests.dec()
        self._pending_start_times.pop(stream_id, None)

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

    def on_invalid_http_method(self, method: str) -> None:
        self._datasource.environment.log.warn(
            "invalid HTTP method",
            str_field("endpoint", self.name),
            str_field("method", method),
        )
        self._invalid_http_method_counter.inc()

    def on_begin_request_failed(self, err: Exception) -> None:
        self._datasource.environment.log.error(
            "begin_request failed",
            str_field("endpoint", self.name),
            err_field(err),
        )
        self._begin_request_failed_counter.inc()


class DataSourceEndpointConsumer[T, R, E](Consumer[T], InputEndpointConsumer):
    _input_stream: TypedInputStream[T, R, E]
    _endpoint: InputEndpoint

    def __init__(self, endpoint: InputEndpoint, input_stream: TypedInputStream[T, R, E]):
        self._endpoint = endpoint
        self._input_stream = input_stream

    async def consume(self, value: T) -> None:
        await self._input_stream.consume(value)

    @property
    def stream(self) -> TypedInputStream[T, R, E]:
        return self._input_stream

    @property
    def endpoint(self) -> InputEndpoint:
        return self._endpoint

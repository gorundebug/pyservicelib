#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the LICENSE file for details.

import time
from dataclasses import dataclass
from typing import Optional

from .environment.metrics import (
    Float64Histogram,
    Float64HistogramVec,
    Int64Gauge,
    Int64GaugeVec,
    Labels,
    Metrics,
)

_DURATION_BUCKETS_SECONDS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
_BODY_SIZE_BUCKETS_BYTES = (
    64.0,
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262144.0,
    1048576.0,
)


@dataclass(frozen=True)
class TransportRequest:
    started_at: float
    active: Optional[Int64Gauge]


class TransportRequestMetrics:
    """Protocol-level metrics consumed by the HTTP and gRPC dashboards."""

    def __init__(
        self,
        metrics: Metrics,
        *,
        prefix: str,
        duration_name: str,
        base_labels: Labels,
        status_label: str,
        success_status: str,
        error_status: str,
        track_active: bool = False,
        track_body_size: bool = False,
    ) -> None:
        scope = metrics.scope(prefix, {})
        self._duration: Float64HistogramVec = scope.histogram_vec(
            duration_name,
            "Duration of a transport request in seconds",
            *_DURATION_BUCKETS_SECONDS,
        )
        self._active: Optional[Int64GaugeVec] = (
            scope.gauge_vec(
                "active_requests",
                "Number of active transport requests",
            )
            if track_active
            else None
        )
        self._request_body_size: Optional[Float64HistogramVec] = (
            scope.histogram_vec(
                "request_body_size_bytes",
                "Size of a transport request body in bytes",
                *_BODY_SIZE_BUCKETS_BYTES,
            )
            if track_body_size
            else None
        )
        self._response_body_size: Optional[Float64HistogramVec] = (
            scope.histogram_vec(
                "response_body_size_bytes",
                "Size of a transport response body in bytes",
                *_BODY_SIZE_BUCKETS_BYTES,
            )
            if track_body_size
            else None
        )
        self._base_labels = base_labels
        self._status_label = status_label
        self._success_status = success_status
        self._error_status = error_status

    def start(self) -> TransportRequest:
        active = (
            self._active.with_(self._base_labels)
            if self._active is not None
            else None
        )
        if active is not None:
            active.inc()
        return TransportRequest(time.monotonic(), active)

    def finish(
        self,
        request: TransportRequest,
        err: Optional[BaseException],
        status: Optional[str] = None,
        request_body_size: Optional[int] = None,
        response_body_size: Optional[int] = None,
    ) -> None:
        if request.active is not None:
            request.active.dec()
        labels = {
            **self._base_labels,
            self._status_label: status
            or (self._success_status if err is None else self._error_status),
        }
        if self._status_label == "http_response_status_code":
            labels["error_type"] = "" if err is None else type(err).__name__
        self._duration.with_(labels).observe(
            time.monotonic() - request.started_at
        )
        self._observe_size(
            self._request_body_size,
            labels,
            request_body_size,
        )
        self._observe_size(
            self._response_body_size,
            labels,
            response_body_size,
        )

    @staticmethod
    def _observe_size(
        histogram: Optional[Float64HistogramVec],
        labels: Labels,
        size: Optional[int],
    ) -> None:
        if histogram is not None and size is not None and size >= 0:
            histogram.with_(labels).observe(float(size))

    @classmethod
    def http_server(
        cls,
        metrics: Metrics,
        *,
        method: str,
        route: str,
        host: str,
        port: int,
    ) -> "TransportRequestMetrics":
        return cls(
            metrics,
            prefix="http_server",
            duration_name="request_duration_seconds",
            base_labels={
                "http_request_method": method,
                "http_route": route,
                "url_scheme": "http",
                "server_address": host,
                "server_port": str(port),
            },
            status_label="http_response_status_code",
            success_status="200",
            error_status="500",
            track_active=True,
            track_body_size=True,
        )

    @classmethod
    def http_client(
        cls,
        metrics: Metrics,
        *,
        method: str,
        route: str,
        server_address: str,
    ) -> "TransportRequestMetrics":
        return cls(
            metrics,
            prefix="http_client",
            duration_name="request_duration_seconds",
            base_labels={
                "http_request_method": method,
                "url_full": route,
                "server_address": server_address,
                "server_port": "",
                "network_protocol_version": "",
            },
            status_label="http_response_status_code",
            success_status="200",
            error_status="500",
            track_body_size=True,
        )

    @classmethod
    def grpc_server(
        cls,
        metrics: Metrics,
        *,
        method: str,
    ) -> "TransportRequestMetrics":
        return cls(
            metrics,
            prefix="rpc_server",
            duration_name="call_duration_seconds",
            base_labels={
                "rpc_system_name": "grpc",
                "rpc_method": method,
            },
            status_label="rpc_response_status_code",
            success_status="OK",
            error_status="UNKNOWN",
        )

    @classmethod
    def grpc_client(
        cls,
        metrics: Metrics,
        *,
        method: str,
    ) -> "TransportRequestMetrics":
        return cls(
            metrics,
            prefix="rpc_client",
            duration_name="call_duration_seconds",
            base_labels={
                "rpc_system_name": "grpc",
                "rpc_method": method,
            },
            status_label="rpc_response_status_code",
            success_status="OK",
            error_status="UNKNOWN",
        )

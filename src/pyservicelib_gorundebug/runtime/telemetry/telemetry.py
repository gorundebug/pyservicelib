#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import os

from ..environment.metrics import MetricsEngine
from ..environment.tracing import TracingEngine
from ..environment.log import LogsEngine
from .opentelemetry import opentelemetrytracing as _tracing
from .opentelemetry import opentelemetrylogging as _logging
from .opentelemetry.opentelemetry import PrometheusMetricsEngine, OTLPMetricsEngine


def create_prometheus_metrics_engine(service_name: str) -> MetricsEngine:
    """Metrics via Prometheus exporter (OTel Prometheus bridge)."""
    return PrometheusMetricsEngine(service_name)


def _otlp_endpoint(endpoint: str | None) -> str:
    if endpoint is not None:
        return endpoint
    return os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'localhost:4317')


def create_otlp_metrics_engine(service_name: str,
                                endpoint: str | None = None,
                                insecure: bool = True) -> MetricsEngine:
    """Metrics exported via OTLP gRPC."""
    return OTLPMetricsEngine(service_name, endpoint=_otlp_endpoint(endpoint), insecure=insecure)


def create_stdout_tracing_engine(service_name: str,
                                  context_sampler: bool = True) -> TracingEngine:
    """Tracing: spans printed to stdout as JSON."""
    return _tracing.create_stdout_tracing_engine(service_name, context_sampler=context_sampler)


def create_otlp_tracing_engine(service_name: str,
                                endpoint: str | None = None,
                                insecure: bool = True,
                                context_sampler: bool = True) -> TracingEngine:
    """Tracing: spans exported via OTLP gRPC."""
    return _tracing.create_otlp_tracing_engine(service_name, endpoint=_otlp_endpoint(endpoint), insecure=insecure,
                                               context_sampler=context_sampler)


def create_pretty_tracing_engine(service_name: str,
                                  context_sampler: bool = True) -> TracingEngine:
    """Tracing: human-readable single-line span output for local debugging."""
    return _tracing.create_pretty_tracing_engine(service_name, context_sampler=context_sampler)


def create_stdout_logs_engine(service_name: str) -> LogsEngine:
    """Logging: structured records written to stdout via OTel."""
    return _logging.create_stdout_logs_engine(service_name)


def create_otlp_logs_engine(service_name: str,
                             endpoint: str | None = None,
                             insecure: bool = True) -> LogsEngine:
    """Logging: structured records exported via OTLP gRPC."""
    return _logging.create_otlp_logs_engine(
        service_name, endpoint=_otlp_endpoint(endpoint), insecure=insecure)

"""Query endpoint handles use configured topology, never SQL or payload values."""
import asyncio
from time import perf_counter
from ..runtime.execution_policy import recording_enabled


class EndpointMetrics:
    def __init__(self, stream):
        environment = stream.environment
        endpoint = environment.config.get_endpoint_config_by_id(stream.endpoint_id)
        connector = environment.config.get_data_connector_by_id(endpoint.id_data_connector)
        scope = environment.metrics.scope("dataquery_endpoint", {
            "connector": connector.name, "endpoint": endpoint.name,
            "operation": endpoint.query_operation,
        })
        self.requests = scope.counter("requests_total", "Admitted Query endpoint requests", {})
        self.results = scope.counter("results_total", "Query result collector emissions, including application completion", {})
        self.errors = scope.counter("failures_total", "Failed Query endpoint requests", {"outcome": "error"})
        self.cancellations = scope.counter("failures_total", "Failed Query endpoint requests", {"outcome": "cancelled"})
        self.active = scope.gauge("active_requests", "Active Query endpoint requests through EndRequest", {})
        self.duration = scope.histogram("request_duration_seconds", "Query request duration through EndRequest in seconds", {})

    def begin(self):
        if not recording_enabled():
            return None
        self.requests.inc()
        self.active.inc()
        return perf_counter()

    def finish(self, started, error, cancelled):
        if started is None:
            return
        self.active.dec()
        if not recording_enabled():
            return
        self.duration.observe(perf_counter() - started)
        if error is not None:
            counter = self.cancellations if cancelled or isinstance(error, (asyncio.CancelledError, TimeoutError)) else self.errors
            counter.inc()

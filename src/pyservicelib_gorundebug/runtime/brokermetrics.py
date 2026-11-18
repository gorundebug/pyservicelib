"""Bounded native publication outcomes; never graph completion or ACK."""
from .config import data_connector_protocol


class PublicationMetrics:
    def __init__(self, endpoint):
        protocol = data_connector_protocol(endpoint.datasink.data_connector.type)
        metrics = endpoint.environment.metrics
        self.counters = {}
        if not metrics.enabled:
            return
        outcomes = ("client-accepted", "flushed", "acknowledged", "error") if protocol in ("nats", "jetstream") else ("not-sent", "accepted", "rejected", "unknown", "unroutable", "modified")
        scope = metrics.scope("broker_endpoint", {
            "protocol": protocol, "connector": endpoint.datasink.name,
            "endpoint": endpoint.name,
        })
        self.counters = {outcome: scope.counter(
            "publication_outcomes_total",
            "Observed transport publication outcomes; not downstream business completion",
            {"outcome": outcome}) for outcome in outcomes}

    def observe(self, outcome):
        # Native SDK vocabularies differ in case; label vocabulary is fixed.
        outcome = "not-sent" if outcome == "NotSent" else outcome.lower()
        if counter := self.counters.get(outcome):
            counter.inc()


class SourceMetrics:
    """Native observations only; never a proxy for ACK or business success."""
    def __init__(self, endpoint):
        self.protocol = data_connector_protocol(endpoint.datasource.data_connector.type)
        self.counters = {}
        metrics = endpoint.environment.metrics
        if not metrics.enabled:
            return
        names = ["received"]
        if self.protocol in ("jetstream", "pulsar", "rabbitmq"):
            names.append("redelivered")
        elif self.protocol == "redisstreams":
            names.append("reclaimed")
        scope = metrics.scope("broker_endpoint", {
            "protocol": self.protocol, "connector": endpoint.datasource.name,
            "endpoint": endpoint.name,
        })
        self.counters = {name: scope.counter(
            "source_observations_total",
            "Observed native source deliveries; not acknowledgements or business completion",
            {"observation": name}) for name in names}

    def observe(self, observation):
        if counter := self.counters.get(observation):
            counter.inc()

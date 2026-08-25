"""Temporal connector and Activity adapters for ordinary input streams."""

from .connector import Connector, EndpointEnvelope, EndpointResult, make_connector

from .temporal import (
    make_direct_endpoint_consumer,
    make_schedule_endpoint_consumer,
)

__all__ = [
    "Connector",
    "EndpointEnvelope",
    "EndpointResult",
    "make_connector",
    "make_direct_endpoint_consumer",
    "make_schedule_endpoint_consumer",
]

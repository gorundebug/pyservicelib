"""Official Temporal SDK integration used by durable graph boundaries."""

from .connector import (
    Connector,
    EndpointEnvelope,
    EndpointResult,
    make_connector,
)

__all__ = [
    "Connector",
    "EndpointEnvelope",
    "EndpointResult",
    "make_connector",
]

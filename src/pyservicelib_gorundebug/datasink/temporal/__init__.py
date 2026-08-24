"""Temporal endpoint submission adapters."""

from .temporal import (
    make_direct_endpoint_consumer,
    make_direct_endpoint_consumer_with_result,
)

__all__ = [
    "make_direct_endpoint_consumer",
    "make_direct_endpoint_consumer_with_result",
]

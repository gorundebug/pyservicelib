"""Temporal Activity adapters for ordinary input streams."""

from .temporal import (
    make_direct_endpoint_consumer,
    make_schedule_endpoint_consumer,
)

__all__ = ["make_direct_endpoint_consumer", "make_schedule_endpoint_consumer"]

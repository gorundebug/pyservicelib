"""Temporal connector and Activity adapters for ordinary input streams."""

from .connector import (
    Connector,
    DirectEndpointWorkflowRequest,
    EndpointEnvelope,
    EndpointResult,
    execute_direct_endpoint_workflow,
    make_connector,
)

from .temporal import (
    EndpointHandler,
    make_direct_endpoint_consumer,
    make_direct_endpoint_consumer_with_handler,
    make_schedule_endpoint_consumer,
)
from .workflow_environment import TemporalWorkflowEnvironment

__all__ = [
    "Connector",
    "DirectEndpointWorkflowRequest",
    "EndpointEnvelope",
    "EndpointHandler",
    "EndpointResult",
    "TemporalWorkflowEnvironment",
    "execute_direct_endpoint_workflow",
    "make_connector",
    "make_direct_endpoint_consumer",
    "make_direct_endpoint_consumer_with_handler",
    "make_schedule_endpoint_consumer",
]

"""Typed configuration constructors for native NATS profiles."""
from typing import Literal
from pydantic import Field
from .config import DataConnectorConfig, EndpointConfig
from ...api.models.data_connector_type import DataConnectorType


class _NatsConnectionConfig(DataConnectorConfig):
    implementation: str = "nats.py"
    servers: list[str] = Field(min_length=1)
    connect_timeout_ms: int = Field(default=2000, alias="connectTimeoutMs", strict=True, ge=1, le=9223372036854)
    operation_timeout_ms: int = Field(default=5000, alias="operationTimeoutMs", strict=True, ge=1, le=9223372036854)


class NatsDataConnectorConfig(_NatsConnectionConfig):
    type: Literal[DataConnectorType.NATS] = DataConnectorType.NATS


class JetStreamDataConnectorConfig(_NatsConnectionConfig):
    type: Literal[DataConnectorType.JetStream] = DataConnectorType.JetStream


class NatsEndpointConfig(EndpointConfig):
    subject: str = Field(min_length=1)


class JetStreamEndpointConfig(NatsEndpointConfig):
    stream_name: str = Field(alias="streamName", min_length=1)

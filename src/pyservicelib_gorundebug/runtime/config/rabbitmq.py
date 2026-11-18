"""Typed native RabbitMQ AMQP 1.0 configuration; resources are opt-in."""

from typing import Literal
from pydantic import Field
from .config import DataConnectorConfig, EndpointConfig
from ...api.models.data_connector_type import DataConnectorType


class RabbitMQDataConnectorConfig(DataConnectorConfig):
    type: Literal[DataConnectorType.RabbitMQ] = DataConnectorType.RabbitMQ
    implementation: str = "rabbitmq-amqp-python-client"
    servers: list[str] = Field(min_length=1)
    connect_timeout_ms: int = Field(
        default=2000, alias="connectTimeoutMs", strict=True, ge=1, le=9223372036854
    )
    operation_timeout_ms: int = Field(
        default=5000, alias="operationTimeoutMs", strict=True, ge=1, le=9223372036854
    )


class RabbitMQEndpointConfig(EndpointConfig):
    consumer_credits: int = Field(
        default=256, alias="consumerCredits", strict=True, ge=1, le=4096
    )
    publisher_max_in_flight: int = Field(
        default=256, alias="publisherMaxInFlight", strict=True, ge=1, le=4096
    )

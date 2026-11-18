"""Standalone Redis Streams configuration. Creation and settlement are explicit."""

import unicodedata
from typing import Literal
from urllib.parse import urlsplit

from pydantic import ConfigDict, Field, model_validator

from ...api.models.data_connector_type import DataConnectorType
from .config import DataConnectorConfig, EndpointConfig


class RedisStreamsDataConnectorConfig(DataConnectorConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    type: Literal[DataConnectorType.RedisStreams] = DataConnectorType.RedisStreams
    implementation: Literal["redis-py"] = "redis-py"
    servers: list[str] = Field(min_length=1, max_length=1)
    redis_database: int = Field(
        default=0, alias="redisDatabase", strict=True, ge=0, le=2147483647
    )
    connect_timeout_ms: int = Field(
        default=2000, alias="connectTimeoutMs", strict=True, ge=1, le=2147483647
    )
    operation_timeout_ms: int = Field(
        default=5000, alias="operationTimeoutMs", strict=True, ge=1, le=2147483647
    )

    @model_validator(mode="after")
    def validate_native(self):
        server = self.servers[0]
        try:
            u = urlsplit(server)
            if (
                u.scheme not in ("redis", "rediss")
                or not u.hostname
                or u.path
                or any(c in server for c in "@?#")
                or u.netloc.endswith(":")
                or any(c.isspace() or unicodedata.category(c) == "Cc" for c in server)
                or (u.port is not None and not 1 <= u.port <= 65535)
            ):
                raise ValueError()
        except ValueError:
            raise ValueError(
                "RedisStreams requires one standalone redis/rediss URL without credentials, path, query or fragment"
            ) from None
        if self.username and not self.password:
            raise ValueError("RedisStreams username requires password")
        if self.token or self.credentials_file:
            raise ValueError("RedisStreams token/credentialsFile unsupported")
        if bool(self.tls_cert_file) != bool(self.tls_key_file):
            raise ValueError("RedisStreams TLS certificate/key must be paired")
        if not (self.tls_enabled or u.scheme == "rediss") and any(
            (
                self.tls_server_name,
                self.tls_ca_file,
                self.tls_cert_file,
                self.tls_key_file,
            )
        ):
            raise ValueError("RedisStreams TLS fields require TLS transport")
        if self.tls_server_name and self.tls_server_name != u.hostname:
            raise ValueError(
                "redis-py public TLS API requires tlsServerName to match URL hostname"
            )
        return self


class RedisStreamsEndpointConfig(EndpointConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    stream_key: str = Field(alias="streamKey", min_length=1, strict=True)
    # Optional defaults preserve absent versus explicit false across source/sink roles.
    create_consumer_group: bool | None = Field(
        default=None, alias="createConsumerGroup", strict=True
    )
    group_start_id: Literal["0-0", "$"] | None = Field(
        default=None, alias="groupStartId"
    )
    read_count: int | None = Field(
        default=None, alias="readCount", strict=True, ge=1, le=4096
    )
    read_block_ms: int | None = Field(
        default=None, alias="readBlockMs", strict=True, ge=1, le=60000
    )
    claim_idle_ms: int | None = Field(
        default=None, alias="claimIdleMs", strict=True, ge=1, le=2147483647
    )
    claim_interval_ms: int | None = Field(
        default=None, alias="claimIntervalMs", strict=True, ge=1, le=2147483647
    )
    create_stream: bool | None = Field(default=None, alias="createStream", strict=True)
    publisher_max_in_flight: int | None = Field(
        default=None, alias="publisherMaxInFlight", strict=True, ge=1, le=4096
    )

    def validate_role(self, source: bool):
        if source:
            if not self.consumer_group:
                raise ValueError("RedisStreams source requires consumerGroup")
            forbidden = ("create_stream", "publisher_max_in_flight")
            if self.group_start_id is not None and not self.create_consumer_group:
                raise ValueError(
                    "RedisStreams groupStartId requires createConsumerGroup"
                )
        else:
            forbidden = (
                "consumer_group",
                "consumer_name",
                "create_consumer_group",
                "group_start_id",
                "read_count",
                "read_block_ms",
                "claim_idle_ms",
                "claim_interval_ms",
            )
        if any(getattr(self, field) is not None for field in forbidden):
            raise ValueError(
                "RedisStreams endpoint has options for the opposite direction"
            )
        unsupported = {
            "queue_group",
            "stream_name",
            "durable_name",
            "create_consumer",
            "consumer_credits",
            "queue_name",
            "queue_type",
            "exchange_name",
            "exchange_type",
            "routing_key",
            "create_queue",
            "create_exchange",
            "create_binding",
            "topic",
            "create_topic",
            "partitions",
            "replication_factor",
            "task_queue",
            "schedule",
            "subject",
        }
        if unsupported.intersection(self.model_fields_set):
            raise ValueError("RedisStreams rejects other broker endpoint options")
        for field in (
            "subject",
            "queue_group",
            "stream_name",
            "durable_name",
            "topic",
            "queue_name",
            "exchange_name",
            "task_queue",
            "schedule",
        ):
            if getattr(self, field, None):
                raise ValueError(
                    "RedisStreams endpoint contains another broker's options"
                )
        return self

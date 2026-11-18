"""Official Pulsar configuration; resource creation is always explicit."""

import unicodedata
from typing import Literal
from urllib.parse import urlsplit

from pydantic import ConfigDict, Field, model_validator

from ...api.models.data_connector_type import DataConnectorType
from .config import DataConnectorConfig, EndpointConfig


def _url(value, schemes):
    try:
        u = urlsplit(value)
        if (
            u.scheme not in schemes
            or not u.hostname
            or u.path
            or any(c in value for c in "@?#")
            or u.netloc.endswith(":")
            or any(c.isspace() or unicodedata.category(c) == "Cc" for c in value)
            or (u.port is not None and not 1 <= u.port <= 65535)
        ):
            raise ValueError()
        return u
    except ValueError:
        raise ValueError(
            "Pulsar URL requires a supported scheme, host and optional port only"
        ) from None


class PulsarDataConnectorConfig(DataConnectorConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    type: Literal[DataConnectorType.Pulsar] = DataConnectorType.Pulsar
    implementation: Literal["pulsar-client-python"] = "pulsar-client-python"
    servers: list[str] = Field(min_length=1, max_length=1)
    admin_url: str = Field(alias="adminUrl")
    admin_token: str = Field(
        default="", alias="adminToken", strict=True, repr=False, exclude=True
    )
    connect_timeout_ms: int = Field(
        default=2000, alias="connectTimeoutMs", strict=True, ge=0, le=2147483647
    )
    operation_timeout_ms: int = Field(
        default=5000, alias="operationTimeoutMs", strict=True, ge=0, le=2147483647
    )

    @model_validator(mode="after")
    def validate_native(self):
        service = _url(self.servers[0], ("pulsar", "pulsar+ssl"))
        admin = _url(self.admin_url, ("http", "https"))
        if service.scheme == "pulsar+ssl" and admin.scheme != "https":
            raise ValueError("Pulsar TLS service requires HTTPS administration")
        if any((self.tls_ca_file, self.tls_cert_file, self.tls_key_file)) and (
            service.scheme != "pulsar+ssl" or admin.scheme != "https"
        ):
            raise ValueError("Pulsar TLS files require TLS service and admin URLs")
        if bool(self.tls_cert_file) != bool(self.tls_key_file):
            raise ValueError("Pulsar TLS certificate/key must be paired")
        if "\r" in self.admin_token or "\n" in self.admin_token:
            raise ValueError(
                "Pulsar adminToken must not contain header control characters"
            )
        if (self.token or self.admin_token) and self.tls_cert_file:
            raise ValueError("Select token or TLS identity")
        if any(
            (
                self.username,
                self.password,
                self.credentials_file,
                self.tls_server_name,
                self.tls_enabled,
            )
        ):
            raise ValueError(
                "Pulsar supports token/TLS identity and URL-controlled TLS only"
            )
        return self

    @property
    def operation_timeout(self):
        return (self.operation_timeout_ms or 5000) / 1000

    @property
    def connect_timeout(self):
        return (self.connect_timeout_ms or 2000) / 1000


class PulsarEndpointConfig(EndpointConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    topic: str = Field(strict=True)
    create_topic: bool = Field(default=False, alias="createTopic", strict=True)
    partitions: int = Field(default=0, strict=True, ge=0, le=1000)
    subscription_name: str | None = Field(default=None, alias="subscriptionName")
    subscription_type: Literal["Shared", "Exclusive", "Failover"] | None = Field(
        default=None, alias="subscriptionType"
    )
    create_subscription: bool | None = Field(
        default=None, alias="createSubscription", strict=True
    )
    subscription_initial_position: Literal["Earliest", "Latest"] | None = Field(
        default=None, alias="subscriptionInitialPosition"
    )
    receiver_queue_size: int | None = Field(
        default=None, alias="receiverQueueSize", strict=True, ge=1, le=4096
    )
    negative_ack_redelivery_delay_ms: int | None = Field(
        default=None,
        alias="negativeAckRedeliveryDelayMs",
        strict=True,
        ge=1,
        le=2147483647,
    )
    publisher_max_in_flight: int | None = Field(
        default=None, alias="publisherMaxInFlight", strict=True, ge=1, le=4096
    )

    def validate_role(self, source):
        parts = self.topic.removeprefix("persistent://").split("/")
        if (
            not self.topic.startswith("persistent://")
            or len(parts) != 3
            or any(
                not p
                or any(c in p for c in "%?#\\")
                or any(c.isspace() or unicodedata.category(c) == "Cc" for c in p)
                for p in parts
            )
        ):
            raise ValueError("Pulsar requires persistent://tenant/namespace/topic")
        if self.partitions and not self.create_topic:
            raise ValueError("Pulsar partitions require createTopic")
        if source:
            if (
                not self.subscription_name
                or any(c in self.subscription_name for c in "/%?#\\")
                or any(unicodedata.category(c) == "Cc" for c in self.subscription_name)
            ):
                raise ValueError("Pulsar source requires valid subscriptionName")
            if self.subscription_initial_position and not self.create_subscription:
                raise ValueError("Pulsar initial position requires createSubscription")
            forbidden = ("publisher_max_in_flight",)
        else:
            forbidden = (
                "subscription_name",
                "subscription_type",
                "create_subscription",
                "subscription_initial_position",
                "receiver_queue_size",
                "negative_ack_redelivery_delay_ms",
            )
        if any(getattr(self, k) is not None for k in forbidden):
            raise ValueError("Pulsar endpoint contains opposite-role options")
        for k in (
            "stream_key",
            "consumer_group",
            "consumer_name",
            "subject",
            "queue_group",
            "stream_name",
            "durable_name",
            "queue_name",
            "exchange_name",
            "routing_key",
            "create_stream",
            "create_consumer",
            "create_queue",
            "create_exchange",
            "create_binding",
            "create_consumer_group",
            "group_start_id",
            "read_count",
            "read_block_ms",
            "claim_idle_ms",
            "claim_interval_ms",
        ):
            if k in self.model_fields_set and getattr(self, k) is not None:
                raise ValueError(
                    "Pulsar endpoint contains unsupported broker option: " + k
                )
        return self

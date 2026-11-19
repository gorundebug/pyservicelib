"""Configuration for independent storage Query connectors/endpoints."""
from typing import Literal

from pydantic import ConfigDict, Field, field_validator
from sqlalchemy.engine import make_url

from ...api.models.data_connector_type import DataConnectorType
from .config import DataConnectorConfig, EndpointConfig


class PostgreSQLSQLAlchemyDataConnectorConfig(DataConnectorConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    type: Literal[DataConnectorType.PostgreSQLSQLAlchemy] = DataConnectorType.PostgreSQLSQLAlchemy
    implementation: Literal['python/sqlalchemy'] = 'python/sqlalchemy'
    connection_string: str = Field(alias='connectionString', repr=False, exclude=True, min_length=1)
    connections_count: int = Field(default=10, alias='connectionsCount', strict=True, ge=1, le=2147483647)
    connect_timeout_ms: int = Field(default=0, alias='connectTimeoutMs', strict=True, ge=0, le=2147483647)
    operation_timeout_ms: int = Field(default=0, alias='operationTimeoutMs', strict=True, ge=0, le=2147483647)

    @field_validator('connection_string')
    @classmethod
    def validate_url(cls, value: str) -> str:
        try:
            url = make_url(value)
            valid = url.drivername in ('postgres', 'postgresql', 'postgresql+asyncpg') and bool(url.database)
        except Exception:
            valid = False
        if not valid:
            raise ValueError('SQLAlchemy Query requires a PostgreSQL connection URL')
        return value


class QueryEndpointConfig(EndpointConfig):
    query_operation: Literal['Rows', 'Execute', 'GetObject', 'HeadObject', 'ListObjects', 'PutObject', 'DeleteObject'] = Field(alias='queryOperation')
    fetch_size: int = Field(default=256, alias='fetchSize', strict=True, ge=1, le=2147483647)
    sql_file: str | None = Field(default=None, alias='sqlFile')
    bucket: str | None = None


class S3AWSDataConnectorConfig(DataConnectorConfig):
    model_config = ConfigDict(hide_input_in_errors=True)
    type: Literal[DataConnectorType.S3AWS] = DataConnectorType.S3AWS
    implementation: Literal['python/aiobotocore'] = 'python/aiobotocore'
    region: str = Field(min_length=1)
    address: str | None = Field(default=None, repr=False, exclude=True)
    force_path_style: bool = Field(default=False, alias='forcePathStyle', strict=True)

    @field_validator('address')
    @classmethod
    def validate_address(cls, value: str | None) -> str | None:
        from urllib.parse import urlsplit
        if not value:
            return None
        try:
            url = urlsplit(value)
            valid = url.scheme in ('http', 'https') and bool(url.hostname) and not any((url.username, url.password, url.query, url.fragment))
            _ = url.port
        except ValueError:
            valid = False
        if not valid:
            raise ValueError('invalid S3 endpoint address')
        return value

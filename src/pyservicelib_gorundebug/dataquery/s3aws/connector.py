"""Connector-owned async S3 transport, using the native credential provider chain."""
import asyncio
from contextlib import AsyncExitStack
from typing import AsyncContextManager, Callable
from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from aiobotocore.config import AioConfig
from .. import _request_execution_scope
from ...runtime.common import ManagedDataConnector, ServiceExecutionEnvironment
from ...runtime.config.query import S3AWSDataConnectorConfig
from ...runtime.context.context import Context
from ...runtime.context.request import request_context_scope


class Connector(ManagedDataConnector):
    def __init__(self, config: S3AWSDataConnectorConfig, *,
                 client_factory: Callable[[Context], AsyncContextManager[AioBaseClient]] | None = None):
        """The factory supplies a fresh client context; the connector owns its exit."""
        self._client_factory = client_factory
        self._config = config.model_copy(deep=True)
        self._lock = asyncio.Lock()
        self._resources = None
        self._client = None

    @property
    def id(self) -> int: return self._config.id

    @property
    def name(self) -> str: return self._config.name

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError('S3 connector is not started')
        return self._client

    async def start(self, ctx: Context) -> None:
        with request_context_scope(ctx):
            async with _request_execution_scope():
                async with self._lock:
                    if self._client is not None:
                        return
                    async with AsyncExitStack() as resources:
                        client_context = self._client_factory(ctx) if self._client_factory is not None else get_session().create_client(
                            's3', region_name=self._config.region,
                            endpoint_url=self._config.address,
                            config=AioConfig(retries={'total_max_attempts': 1, 'mode': 'standard'},
                                s3={'addressing_style': 'path' if self._config.force_path_style else 'auto'}),
                        )
                        client = await resources.enter_async_context(client_context)
                        self._resources = resources.pop_all()
                        self._client = client

    async def start_admission(self, ctx: Context) -> None: pass
    async def stop_admission(self, ctx: Context) -> None: pass

    async def stop(self, ctx: Context) -> None:
        async with self._lock:
            resources = self._resources
            self._resources = self._client = None
            if resources is not None:
                await resources.aclose()


def make_connector(environment: ServiceExecutionEnvironment, config: S3AWSDataConnectorConfig) -> Connector:
    existing = environment.get_managed_data_connector(config.id)
    if existing is not None:
        if not isinstance(existing, Connector):
            raise ValueError(f'connector {config.id} already has another runtime type')
        return existing
    connector = Connector(config)
    environment.add_managed_data_connector(connector)
    return connector

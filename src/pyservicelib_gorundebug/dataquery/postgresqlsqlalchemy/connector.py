"""One SQLAlchemy pool registered in the standard managed connector lifecycle."""
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .. import _request_execution_scope
from ...runtime.common import ManagedDataConnector, ServiceExecutionEnvironment
from ...runtime.config.query import PostgreSQLSQLAlchemyDataConnectorConfig
from ...runtime.context.context import Context
from ...runtime.context.request import request_context_scope


class Connector(ManagedDataConnector):
    def __init__(self, config: PostgreSQLSQLAlchemyDataConnectorConfig, *, engine: AsyncEngine | None = None):
        """A supplied engine transfers ownership; stop disposes its pool."""
        self._id, self._name = config.id, config.name
        self.operation_timeout_ms = config.operation_timeout_ms
        if engine is not None:
            self.engine = engine
            return
        arguments = {}
        if config.connect_timeout_ms:
            arguments['timeout'] = config.connect_timeout_ms / 1000
        if config.operation_timeout_ms:
            arguments['command_timeout'] = config.operation_timeout_ms / 1000
        url = make_url(config.connection_string).set(drivername='postgresql+asyncpg')
        self.engine: AsyncEngine = create_async_engine(
            url, pool_size=config.connections_count, max_overflow=0,
            connect_args=arguments, hide_parameters=True,
        )

    @property
    def id(self) -> int: return self._id

    @property
    def name(self) -> str: return self._name

    async def start(self, ctx: Context) -> None:
        with request_context_scope(ctx):
            async with _request_execution_scope():
                async with self.engine.connect() as connection:
                    await connection.execute(text('SELECT 1'))

    async def start_admission(self, ctx: Context) -> None:
        pass

    async def stop_admission(self, ctx: Context) -> None:
        pass

    async def stop(self, ctx: Context) -> None:
        await self.engine.dispose()


def make_connector(environment: ServiceExecutionEnvironment, config: PostgreSQLSQLAlchemyDataConnectorConfig) -> Connector:
    existing = environment.get_managed_data_connector(config.id)
    if existing is not None:
        if not isinstance(existing, Connector):
            raise ValueError(f'connector {config.id} already has another runtime type')
        return existing
    connector = Connector(config)
    environment.add_managed_data_connector(connector)
    return connector

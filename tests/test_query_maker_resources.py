import unittest
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine
from pyservicelib_gorundebug.dataquery.postgresqlsqlalchemy.connector import Connector as Postgres
from pyservicelib_gorundebug.dataquery.s3aws.connector import Connector as S3
from pyservicelib_gorundebug.runtime.config.query import PostgreSQLSQLAlchemyDataConnectorConfig, S3AWSDataConnectorConfig
from pyservicelib_gorundebug.runtime.context.context import Context

class MakerResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_engine_owned_by_connector(self):
        engine = create_async_engine('postgresql+asyncpg://localhost/test', pool_size=7)
        config = PostgreSQLSQLAlchemyDataConnectorConfig(id=1, name='db', connectionString='postgresql://localhost/test')
        connector = Postgres(config, engine=engine)
        self.assertIs(connector.engine, engine)
        self.assertEqual(engine.pool.size(), 7)
        pool = engine.pool
        await connector.stop(Context())
        self.assertIsNot(engine.pool, pool)

    async def test_native_s3_context_lifetime(self):
        events = []
        native = object()
        ctx = Context()
        @asynccontextmanager
        async def factory(context):
            self.assertIs(context, ctx)
            events.append('enter')
            try:
                yield native
            finally:
                events.append('exit')
        connector = S3(S3AWSDataConnectorConfig(id=2, name='s3', region='eu-west-1'), client_factory=factory)
        for _ in range(2):
            await connector.start(ctx)
            await connector.start(ctx)
            self.assertIs(connector.client, native)
            await connector.stop(ctx)
            await connector.stop(ctx)
            with self.assertRaises(RuntimeError):
                _ = connector.client
        self.assertEqual(events, ['enter', 'exit', 'enter', 'exit'])

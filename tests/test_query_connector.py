import unittest

from pydantic import ValidationError

from pyservicelib_gorundebug.dataquery.postgresqlsqlalchemy.connector import make_connector
from pyservicelib_gorundebug.runtime.config.query import PostgreSQLSQLAlchemyDataConnectorConfig, QueryEndpointConfig
from pyservicelib_gorundebug.runtime.context.context import Context


class Environment:
    def __init__(self): self.connectors = {}
    def get_managed_data_connector(self, ident): return self.connectors.get(ident)
    def add_managed_data_connector(self, connector): self.connectors[connector.id] = connector


class QueryConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_registration_and_cancelled_start(self):
        env = Environment()
        config = PostgreSQLSQLAlchemyDataConnectorConfig(
            id=12, name='Database', connectionString='postgresql://user:secret@127.0.0.1/test',
            connectionsCount=3, connectTimeoutMs=300, operationTimeoutMs=500,
        )
        connector = make_connector(env, config)
        self.addAsyncCleanup(connector.stop, Context())
        self.assertIs(make_connector(env, config), connector)
        self.assertEqual(connector.engine.pool.size(), 3)
        self.assertNotIn('secret', repr(config))
        self.assertNotIn('secret', str(config.model_dump()))
        self.assertNotIn('secret', str(config.to_dict()))
        cancelled = Context()
        cancelled.cancel()
        # No connection attempt should begin after the lifecycle was cancelled.
        import asyncio
        with self.assertRaises(asyncio.CancelledError):
            await connector.start(cancelled)
        asyncio.current_task().uncancel()
        self.assertEqual(connector.engine.pool.checkedout(), 0)
        env.connectors[12] = object()
        with self.assertRaisesRegex(ValueError, 'another runtime type'):
            make_connector(env, config)

    async def test_configuration_validation(self):
        for value in ('mysql://u:private-value@localhost/db', 'invalid-private-value'):
            with self.assertRaises(ValidationError) as error:
                PostgreSQLSQLAlchemyDataConnectorConfig(id=1, name='Database', connectionString=value)
            self.assertNotIn('private-value', str(error.exception))
        endpoint = QueryEndpointConfig(id=2, name='Rows', idDataConnector=1,
            queryOperation='Rows', functionName='ReadRows', fetchSize=64, enabled=False)
        self.assertFalse(endpoint.enabled)
        self.assertEqual(endpoint.fetch_size, 64)
        self.assertEqual(endpoint.query_operation, 'Rows')

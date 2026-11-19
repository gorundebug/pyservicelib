"""Default SQLAlchemy/asyncpg connector native TLS settings."""
import os
import ssl
import unittest
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from sqlalchemy import text
from pyservicelib_gorundebug.dataquery.postgresqlsqlalchemy.connector import Connector
from pyservicelib_gorundebug.runtime.config.query import PostgreSQLSQLAlchemyDataConnectorConfig
from pyservicelib_gorundebug.runtime.context.context import Context

@unittest.skipUnless(os.getenv('SA_QUERY_TLS_POSTGRES_DSN'), 'requires disposable PostgreSQL TLS')
class PostgreSQLTLSTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_verification(self):
        dsn = urlsplit(os.environ['SA_QUERY_TLS_POSTGRES_DSN'])
        params = dict(parse_qsl(dsn.query))
        ca = params.pop('sslrootcert')
        params.pop('sslmode', None)
        # SQLAlchemy passes asyncpg keyword args, not libpq URI options.
        params['ssl'] = 'verify-full'
        address = urlunsplit(dsn._replace(query=urlencode(params)))
        old_ca = os.environ.get('PGSSLROOTCERT')
        context = Context()
        try:
            for trusted in (False, True):
                with self.subTest(trusted=trusted):
                    os.environ['PGSSLROOTCERT'] = ca if trusted else os.environ['SA_QUERY_TLS_BAD_CA']
                    connector = Connector(PostgreSQLSQLAlchemyDataConnectorConfig(id=1, name='tls',
                        connectionString=address, connectionsCount=1, connectTimeoutMs=2000, operationTimeoutMs=3000))
                    try:
                        if not trusted:
                            with self.assertRaises(ssl.SSLCertVerificationError):
                                await connector.start(context)
                        else:
                            await connector.start(context)
                            async with connector.engine.connect() as conn:
                                self.assertTrue((await conn.execute(text('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()'))).scalar_one())
                    finally: await connector.stop(context)
        finally:
            if old_ca is None: os.environ.pop('PGSSLROOTCERT',None)
            else: os.environ['PGSSLROOTCERT'] = old_ca

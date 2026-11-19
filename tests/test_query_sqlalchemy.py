from types import SimpleNamespace
from pyservicelib_gorundebug.runtime.environment.metrics import NoopMetrics
import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from pyservicelib_gorundebug.runtime.context.request import request_deadline, request_cancelled

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from pyservicelib_gorundebug.dataquery import BeginRequest
from pyservicelib_gorundebug.dataquery.postgresqlsqlalchemy import (
    Query, make_rows_endpoint_consumer, make_execute_endpoint_consumer,
)


class Stream:
    environment = SimpleNamespace(metrics=NoopMetrics())
    def __init__(self):
        self.values = []
        self.errors = []
        self.error_stream = self
    async def consume_result(self, value): self.values.append(value)
    async def consume(self, value): self.errors.append(value)
    def set_query_consumer(self, consumer): self.consumer = consumer


@unittest.skipUnless(os.getenv('SA_QUERY_TEST_POSTGRES_DSN'), 'requires disposable PostgreSQL')
class SQLAlchemyQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_builder_and_raw_returning_preserve_types_and_rollback(self):
        from datetime import datetime, timezone
        from decimal import Decimal
        from uuid import UUID
        from sqlalchemy import (MetaData, Table, Column, Text, Numeric, BigInteger,
                                DateTime, LargeBinary, Uuid, bindparam, insert, select, func)
        from sqlalchemy.dialects.postgresql import JSONB
        engine = create_async_engine(os.environ['SA_QUERY_TEST_POSTGRES_DSN'], pool_size=1, max_overflow=0)
        self.addAsyncCleanup(engine.dispose)
        table = Table('query_builder_values', MetaData(),
            Column('id', Text, primary_key=True), Column('optional', Text),
            Column('amount', Numeric(38, 18)), Column('large', BigInteger),
            Column('instant', DateTime(timezone=True)), Column('uuid', Uuid),
            Column('body', LargeBinary), Column('document', JSONB))
        async with engine.begin() as connection:
            await connection.execute(text('CREATE TEMP TABLE query_builder_values '
                '(id text PRIMARY KEY, optional text, amount numeric(38,18), large bigint, '
                'instant timestamptz, uuid uuid, body bytea, document jsonb)'))
        owner = self
        stored = 0
        for raw in (False, True):
            for fail in (False, True):
                with self.subTest(raw=raw, fail=fail):
                    values = dict(id=f"bound-'key-{raw}-{fail}", optional=None,
                        amount=Decimal('12345678901234567890.123456789012345678'),
                        large=9223372036854775807,
                        instant=datetime(2026, 11, 19, 12, 34, 56, 123456, tzinfo=timezone.utc),
                        uuid=UUID('12345678-1234-5678-1234-567812345678'),
                        body=b'a\x00\xff', document={'value': [1, None, 'quoted"']})
                    stream, outcomes = Stream(), []
                    class Handler:
                        async def begin_request(self, stream): return BeginRequest(None)
                        async def build_query(self, state, value):
                            if raw:
                                statement = text('INSERT INTO query_builder_values '
                                    '(id,optional,amount,large,instant,uuid,body,document) VALUES '
                                    '(:id,:optional,:amount,:large,:instant,:uuid,:body,:document) RETURNING *')
                                statement = statement.bindparams(*[bindparam(c.name, type_=c.type) for c in table.columns])
                                return Query(statement, value)
                            return Query(insert(table).values(**value).returning(*table.columns))
                        async def handle_row(self, state, row, out):
                            await out.out(dict(row._mapping))
                            if fail: raise ValueError('intentional mapper failure')
                        async def end_request(self, stream, state, error, out, errors):
                            outcomes.append(error)
                            owner.assertEqual(engine.sync_engine.pool.checkedout(), 0)
                            if error is not None: await errors.out(error)
                            await out.out('done')
                    await make_rows_endpoint_consumer(stream, engine, Handler(), fetch_size=1).consume(values)
                    self.assertEqual(stream.values, [values, 'done'])
                    self.assertEqual(len(outcomes), 1)
                    self.assertEqual(len(stream.errors), int(fail))
                    if fail: self.assertIsInstance(outcomes[0], ValueError)
                    else:
                        self.assertIsNone(outcomes[0])
                        stored += 1
                    async with engine.connect() as connection:
                        self.assertEqual(await connection.scalar(select(func.count()).select_from(table)), stored)
                        record = (await connection.execute(select(table).where(table.c.id == values['id']))).mappings().one_or_none()
                        self.assertEqual(dict(record) if record is not None else None, None if fail else values)

    async def test_large_rows_wait_for_result_consumer(self):
        engine = create_async_engine(os.environ['SA_QUERY_TEST_POSTGRES_DSN'], pool_size=1, max_overflow=0)
        self.addAsyncCleanup(engine.dispose)
        owner = self
        entered, release = asyncio.Event(), asyncio.Event()
        mapped, delivered, ends = 0, 0, 0
        class GatedStream(Stream):
            async def consume_result(self, value):
                nonlocal delivered
                delivered += 1
                owner.assertEqual(value, delivered)
                if delivered == 1:
                    entered.set()
                    await release.wait()
        class Handler:
            async def begin_request(self, stream): return BeginRequest(None)
            async def build_query(self, state, count):
                return Query(text('SELECT generate_series(1, :count)'), {'count': count})
            async def handle_row(self, state, row, out):
                nonlocal mapped
                mapped += 1
                await out.out(row[0])
            async def end_request(self, stream, state, error, out, errors):
                nonlocal ends
                ends += 1
                owner.assertIsNone(error)
                owner.assertEqual(engine.sync_engine.pool.checkedout(), 0)
        consumer = make_rows_endpoint_consumer(GatedStream(), engine, Handler(), fetch_size=32)
        async with asyncio.timeout(10):
            pending = asyncio.create_task(consumer.consume(10000))
            try:
                await entered.wait()
                for _ in range(5): await asyncio.sleep(0)
                self.assertEqual((mapped, delivered, ends), (1, 1, 0))
                release.set()
                await pending
                self.assertEqual((mapped, delivered, ends), (10000, 10000, 1))
                async with engine.connect() as connection:
                    self.assertEqual(await connection.scalar(text('SELECT 1')), 1)
            finally:
                release.set()
                await pending

    async def test_queries(self):
        engine = create_async_engine(os.environ['SA_QUERY_TEST_POSTGRES_DSN'], pool_size=1, max_overflow=0)
        self.addAsyncCleanup(engine.dispose)
        owner = self
        for mode in ('empty', 'many', 'partial', 'cancel', 'sql_error'):
            with self.subTest(mode=mode):
                stream = Stream()
                calls = []
                class Handler:
                    async def begin_request(self, stream): return BeginRequest(42)
                    async def build_query(self, state, count):
                        owner.assertEqual(state, 42)
                        return Query(text('SELECT missing_column' if mode == 'sql_error' else 'SELECT generate_series(1, :count)'), {'count': count})
                    async def handle_row(self, state, row, out):
                        await out.out(row[0])
                        if mode == 'partial': raise ValueError('mapping failure')
                        if mode == 'cancel': raise asyncio.CancelledError()
                    async def end_request(self, stream, state, error, out, errors):
                        calls.append('end')
                        owner.assertEqual(engine.sync_engine.pool.checkedout(), 0)
                        if error is not None: await errors.out(error)
                        await out.out('done')
                consumer = make_rows_endpoint_consumer(stream, engine, Handler(), fetch_size=2)
                if mode == 'cancel':
                    with self.assertRaises(asyncio.CancelledError): await consumer.consume(3)
                else:
                    await consumer.consume(0 if mode == 'empty' else 3)
                self.assertEqual(calls, ['end'])
                self.assertEqual(stream.values, [1, 2, 3, 'done'] if mode == 'many' else [1, 'done'] if mode in ('partial', 'cancel') else ['done'])
                self.assertEqual(len(stream.errors), int(mode not in ('empty', 'many')))

        async with engine.begin() as connection:
            await connection.execute(text('CREATE TEMP TABLE query_values (value integer)'))
        stream = Stream()
        class ExecuteHandler:
            async def begin_request(self, stream): return BeginRequest(None)
            async def build_query(self, state, value): return Query(text('INSERT INTO query_values VALUES (:value)'), {'value': value})
            async def handle_result(self, state, result, out):
                # A new borrow sees the committed value before output mapping.
                async with engine.connect() as connection:
                    value = await connection.scalar(text('SELECT value FROM query_values'))
                owner.assertEqual(value, 42)
                await out.out(result.rowcount)
            async def end_request(self, stream, state, error, out, errors):
                owner.assertIsNone(error)
                owner.assertEqual(engine.sync_engine.pool.checkedout(), 0)
                await out.out('done')
        await make_execute_endpoint_consumer(stream, engine, ExecuteHandler()).consume(42)
        self.assertEqual(stream.values, [1, 'done'])

    async def test_inflight_deadline_and_cancellation(self):
        engine = create_async_engine(os.environ['SA_QUERY_TEST_POSTGRES_DSN'], pool_size=1, max_overflow=0)
        self.addAsyncCleanup(engine.dispose)
        owner = self
        for mode in ('deadline', 'signal'):
            with self.subTest(mode=mode):
                stream = Stream()
                begun = asyncio.Event()
                signal = asyncio.Event()
                ends = []
                class Handler:
                    async def begin_request(self, stream): return BeginRequest(None)
                    async def build_query(self, state, value):
                        begun.set()
                        return Query(text('SELECT pg_sleep(10)'))
                    async def handle_row(self, state, row, out): owner.fail('cancelled query emitted row')
                    async def end_request(self, stream, state, error, out, errors):
                        ends.append(error)
                        owner.assertEqual(engine.sync_engine.pool.checkedout(), 0)
                deadline_token = request_deadline.set(datetime.now(timezone.utc) + timedelta(milliseconds=150) if mode == 'deadline' else None)
                signal_token = request_cancelled.set(signal if mode == 'signal' else None)
                async def cancel_after_start():
                    await begun.wait()
                    await asyncio.sleep(0.15)
                    signal.set()
                observer = asyncio.create_task(cancel_after_start()) if mode == 'signal' else None
                try:
                    consumer = make_rows_endpoint_consumer(stream, engine, Handler())
                    if mode == 'signal':
                        with self.assertRaises(asyncio.CancelledError): await consumer.consume(1)
                        asyncio.current_task().uncancel()
                    else:
                        await consumer.consume(1)
                    self.assertEqual(len(ends), 1)
                    self.assertIsInstance(ends[0], TimeoutError if mode == 'deadline' else asyncio.CancelledError)
                finally:
                    request_deadline.reset(deadline_token)
                    request_cancelled.reset(signal_token)
                    if observer is not None: await observer

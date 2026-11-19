from types import SimpleNamespace
from pyservicelib_gorundebug.runtime.environment.metrics import NoopMetrics
import asyncio
import unittest
from pyservicelib_gorundebug.dataquery import BeginRequest, make_endpoint_consumer
from pyservicelib_gorundebug.runtime.context.request import request_cancelled, request_stream_id


class Output:
    def __init__(self):
        self.values = []
    async def consume(self, value):
        self.values.append(value)


class Stream:
    environment = SimpleNamespace(metrics=NoopMetrics())
    def __init__(self):
        self.values = []
        self.error_stream = Output()
    async def consume_result(self, value):
        self.values.append(value)
    def set_query_consumer(self, consumer):
        self.consumer = consumer


class QueryLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_late_cancellation_finalizes_after_cleanup_without_replay(self):
        import time
        for mode in ('signal', 'timeout'):
            with self.subTest(mode=mode):
                stream, order, outcomes = Stream(), [], []
                signal = asyncio.Event()
                class Handler:
                    async def begin_request(self, stream): return BeginRequest('state')
                    async def end_request(self, stream, state, error, out, errors):
                        order.append('end')
                        outcomes.append(error)
                async def execute(state, value, out):
                    try:
                        order.append('write')
                        if mode == 'signal': signal.set()
                        else: time.sleep(0.03)  # Deliberately non-yielding application mapper.
                    finally:
                        order.append('closed')
                consumer = make_endpoint_consumer(stream, Handler(), execute,
                    operation_timeout_ms=5 if mode == 'timeout' else 0)
                token = request_cancelled.set(signal)
                try:
                    if mode == 'signal':
                        with self.assertRaises(asyncio.CancelledError):
                            await consumer.consume(None)
                    else:
                        await consumer.consume(None)
                finally:
                    request_cancelled.reset(token)
                self.assertEqual(order, ['write', 'closed', 'end'])
                self.assertEqual(len(outcomes), 1)
                self.assertIsInstance(outcomes[0], asyncio.CancelledError if mode == 'signal' else TimeoutError)

    async def test_lifecycle(self):
        failure = ValueError('storage failure')
        for mode in ('empty', 'many', 'partial', 'begin', 'cancel', 'task_cancel'):
            with self.subTest(mode=mode):
                stream = Stream()
                calls = []
                class Handler:
                    async def begin_request(self, stream):
                        calls.append('begin')
                        return BeginRequest(41, failure if mode == 'begin' else None)
                    async def end_request(self, stream, state, error, out, errors):
                        calls.append('end')
                        self_case.assertEqual(state, 41)
                        self_case.assertEqual(request_stream_id.get(), 'parent')
                        if error is not None:
                            await errors.out(error)
                        await out.out('done')
                self_case = self
                async def execute(state, value, out):
                    calls.append('execute')
                    self.assertEqual((state, value), (41, 42))
                    if mode == 'task_cancel':
                        raise asyncio.CancelledError()
                    if mode == 'empty':
                        return
                    await out.out('row')
                    if mode == 'partial':
                        raise failure
                    await out.out('row')
                consumer = make_endpoint_consumer(stream, Handler(), execute)
                signal = asyncio.Event()
                if mode == 'cancel': signal.set()
                token = request_cancelled.set(signal)
                stream_token = request_stream_id.set('parent')
                try:
                    if mode == 'task_cancel':
                        with self.assertRaises(asyncio.CancelledError):
                            await consumer.consume(42)
                    else:
                        await consumer.consume(42)
                finally:
                    request_stream_id.reset(stream_token)
                    request_cancelled.reset(token)
                self.assertEqual(calls.count('end'), 1)
                self.assertEqual('execute' in calls, mode not in ('begin', 'cancel'))
                self.assertEqual(stream.values, ['row', 'row', 'done'] if mode == 'many' else ['row', 'done'] if mode == 'partial' else ['done'])
                self.assertEqual(len(stream.error_stream.values), 0 if mode in ('empty', 'many') else 1)

    async def test_task_cancellation_closes_adapter_before_end_request(self):
        stream = Stream()
        started = asyncio.Event()
        waiting = asyncio.Event()
        order = []

        class Handler:
            async def begin_request(self, stream):
                return BeginRequest('request')
            async def end_request(self, stream, state, error, out, errors):
                self_case.assertIsInstance(error, asyncio.CancelledError)
                order.append('end')
                await out.out('cancelled')

        self_case = self
        async def execute(state, value, out):
            try:
                started.set()
                await waiting.wait()
            finally:
                order.append('close')

        consumer = make_endpoint_consumer(stream, Handler(), execute)
        task = asyncio.create_task(consumer.consume(42))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(order, ['close', 'end'])
        self.assertEqual(stream.values, ['cancelled'])

    async def test_operation_timeout_caps_execution_and_preserves_finalization(self):
        from datetime import datetime, timedelta, timezone
        from pyservicelib_gorundebug.runtime.context.request import request_deadline
        for configured, inherited in ((20, 1000), (1000, 20), (0, 20)):
            with self.subTest(configured=configured, inherited=inherited):
                stream, order, outcomes = Stream(), [], []
                parent_deadline = datetime.now(timezone.utc) + timedelta(milliseconds=inherited)
                class Handler:
                    async def begin_request(self, stream): return BeginRequest('state')
                    async def end_request(self, stream, state, error, out, errors):
                        order.append('end')
                        outcomes.append(error)
                        await asyncio.sleep(0)
                        await out.out('done')
                async def execute(state, value, out):
                    try:
                        await out.out('first')
                        await asyncio.Event().wait()
                    finally:
                        order.append('closed')
                consumer = make_endpoint_consumer(stream, Handler(), execute,
                    operation_timeout_ms=configured)
                token = request_deadline.set(parent_deadline)
                try:
                    await asyncio.wait_for(consumer.consume(None), timeout=0.5)
                    self.assertEqual(request_deadline.get(), parent_deadline)
                finally:
                    request_deadline.reset(token)
                self.assertEqual(order, ['closed', 'end'])
                self.assertEqual(stream.values, ['first', 'done'])
                self.assertEqual(len(outcomes), 1)
                self.assertIsInstance(outcomes[0], TimeoutError)

    async def test_operation_timeout_rejects_invalid_configuration(self):
        async def execute(state, value, out): pass
        for value in (-1, True, 1.5, '20'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                make_endpoint_consumer(Stream(), object(), execute, operation_timeout_ms=value)

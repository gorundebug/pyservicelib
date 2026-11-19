import asyncio
from types import SimpleNamespace
import pytest
from pyservicelib_gorundebug.dataquery import BeginRequest, make_endpoint_consumer
from pyservicelib_gorundebug.runtime.testmetrics import TestMetrics as Registry
from pyservicelib_gorundebug.runtime.execution_policy import recording_policy_scope
from .test_query_lifecycle import Stream


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['empty', 'results', 'partial', 'cancel', 'concurrent', 'replay'])
async def test_endpoint_metrics(mode):
    metrics = Registry()
    labels = dict(connector='database', endpoint='rows', operation='Rows')
    active = lambda: metrics.gauge('dataquery_endpoint_active_requests', labels).value()
    stream = Stream()
    stream.endpoint_id = 2
    stream.environment = SimpleNamespace(metrics=metrics, config=SimpleNamespace(
        get_endpoint_config_by_id=lambda _: SimpleNamespace(name='rows', id_data_connector=1, query_operation='Rows'),
        get_data_connector_by_id=lambda _: SimpleNamespace(name='database'),
    ))
    count = 2 if mode == 'concurrent' else 1
    barrier = asyncio.Event()
    entered = 0

    class Handler:
        async def begin_request(self, stream):
            return BeginRequest(0)
        async def end_request(self, stream, state, error, out, errors):
            assert (active() == 0 if mode == 'replay' else active() > 0)
            if mode not in ('partial', 'cancel'):
                assert error is None
            await out.out('completion')

    async def execute(state, value, out):
        nonlocal entered
        if mode == 'concurrent':
            entered += 1
            if entered == count:
                assert active() == count
                barrier.set()
            await barrier.wait()
        if mode == 'cancel':
            raise asyncio.CancelledError()
        if mode != 'empty':
            await out.out('row')
        if mode == 'partial':
            raise ValueError('storage failure')

    consumer = make_endpoint_consumer(stream, Handler(), execute)
    with recording_policy_scope(lambda: mode != 'replay'):
        outcomes = await asyncio.wait_for(asyncio.gather(*(consumer.consume(1) for _ in range(count)), return_exceptions=True), 2)
        for outcome in outcomes:
            if mode == 'cancel':
                assert isinstance(outcome, asyncio.CancelledError)
            else:
                assert outcome is None
    assert active() == 0
    expected = 0 if mode == 'replay' else count
    assert metrics.counter('dataquery_endpoint_requests_total', labels).count() == expected
    assert metrics.counter('dataquery_endpoint_results_total', labels).count() == expected * (1 if mode in ('empty','cancel') else 2)
    assert metrics.histogram('dataquery_endpoint_request_duration_seconds', labels).count() == expected
    for outcome in ('error', 'cancelled'):
        expected = int((mode == 'partial' and outcome == 'error') or (mode == 'cancel' and outcome == 'cancelled'))
        assert metrics.counter('dataquery_endpoint_failures_total', dict(labels, outcome=outcome)).count() == expected

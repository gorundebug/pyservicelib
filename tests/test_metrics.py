#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import pytest

from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetricsEngine
from pyservicelib_gorundebug.runtime.telemetry import create_prometheus_metrics_engine


@pytest.mark.asyncio
async def test_noop_metrics_discards_all_observations():
    engine = NoopMetricsEngine()
    scope = engine.metrics.scope("noop", {"service": "test"})
    scope.counter("requests", "Requests", {}).inc()
    scope.gauge("active", "Active", {}).set(42)
    scope.histogram("latency", "Latency", {}).observe(1.5)
    scope.observable_float64_gauge("value", "Value", lambda: 10.0)
    assert engine.metrics_handler() == b""
    await engine.shutdown()


@pytest.mark.asyncio
async def test_metrics_scope_counter():
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test', {'service': 'test_svc'})

    counter = scope.counter('requests', 'Total requests', {})
    counter.inc()
    counter.add(5)


@pytest.mark.asyncio
async def test_metrics_scope_counter_vec():
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test2', {'service': 'test_svc'})

    vec = scope.counter_vec('calls', 'Total calls by status')
    c1 = vec.with_({'status': '200'})
    c2 = vec.with_({'status': '500'})
    c1.inc()
    c2.add(3)
    c1.inc()


@pytest.mark.asyncio
async def test_metrics_scope_gauge():
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test3', {})

    g = scope.gauge('queue_size', 'Current queue size', {'pool': 'default'})
    g.set(10)
    g.inc()
    g.dec()
    g.add(5)
    g.sub(2)


@pytest.mark.asyncio
async def test_metrics_scope_gauge_vec():
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test4', {})

    gv = scope.gauge_vec('active_tasks', 'Active tasks per pool')
    g1 = gv.with_({'pool': 'fast'})
    g2 = gv.with_({'pool': 'slow'})
    g1.set(3)
    g2.inc()
    gv.delete({'pool': 'fast'})


@pytest.mark.asyncio
async def test_metrics_scope_histogram():
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test5', {})

    h = scope.histogram('latency', 'Request latency', {}, 0.01, 0.05, 0.1, 0.5, 1.0)
    h.observe(0.03)
    h.observe(0.5)


@pytest.mark.asyncio
async def test_metrics_scope_observable_gauge():
    import time
    engine = create_prometheus_metrics_engine()
    metrics = engine.metrics
    scope = metrics.scope('test6', {})

    start = time.time()
    scope.observable_float64_gauge('uptime_seconds', 'Service uptime', lambda: time.time() - start)


@pytest.mark.asyncio
async def test_metrics_handler_bytes():
    engine = create_prometheus_metrics_engine()
    data = engine.metrics_handler()
    assert isinstance(data, bytes)

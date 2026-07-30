#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import contextvars

import pytest

from pyservicelib_gorundebug.runtime.environment.tracing import (
    enable_sampling, sampling_enabled,
    start_span, span_event, span_error, span_attrs,
    string_attr, int64_attr, NOOP_SPAN,
)
from pyservicelib_gorundebug.runtime.telemetry.opentelemetry.opentelemetrytracing import (
    create_stdout_tracing_engine,
)


def _run_isolated(fn):
    """Run fn in a fresh copy of the current context (ContextVar isolation)."""
    return contextvars.copy_context().run(fn)


def test_sampling_disabled_by_default():
    assert not sampling_enabled()


def test_enable_sampling_isolated():
    results = {}

    def task():
        results['before'] = sampling_enabled()
        enable_sampling()
        results['after'] = sampling_enabled()

    _run_isolated(task)

    assert results['before'] is False
    assert results['after'] is True
    assert not sampling_enabled()   # outer context unchanged


def test_start_span_no_tracer_returns_noop():
    ctx, span = start_span(None, 'op')
    assert span is NOOP_SPAN
    span.end()


def test_start_span_sampling_off_returns_noop():
    # Opt-in sampling is the production default, not merely an optional mode.
    eng = create_stdout_tracing_engine('test-service')
    tracer = eng.tracing.tracer('test')

    result = {}

    def task():
        result['ctx'], result['span'] = start_span(tracer, 'op')

    _run_isolated(task)
    assert result['span'] is NOOP_SPAN


def test_start_span_sampling_on():
    eng = create_stdout_tracing_engine('test-service', context_sampler=False)
    tracer = eng.tracing.tracer('test')

    result = {}

    def task():
        enable_sampling()
        result['ctx'], result['span'] = start_span(tracer, 'my_op', string_attr('key', 'val'))

    _run_isolated(task)

    span = result['span']
    assert span is not NOOP_SPAN
    sc = span.span_context()
    assert sc.is_valid
    span.set_attributes(int64_attr('count', 42))
    span.add_event('checkpoint', string_attr('stage', 'A'))
    span.end()


def test_span_helpers_on_none():
    span_event(None, 'ev')
    span_error(None, RuntimeError('oops'))
    span_attrs(None, string_attr('k', 'v'))


def test_noop_span_all_methods():
    NOOP_SPAN.end()
    NOOP_SPAN.set_attributes(string_attr('a', 'b'))
    NOOP_SPAN.record_error(RuntimeError('x'))
    NOOP_SPAN.set_status(0, '')
    NOOP_SPAN.add_event('e')
    sc = NOOP_SPAN.span_context()
    assert not sc.is_valid


@pytest.mark.asyncio
async def test_tracing_engine_shutdown():
    eng = create_stdout_tracing_engine('shutdown-test')
    await eng.shutdown()

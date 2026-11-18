"""Native byte headers must not make optional trace extraction reject input."""
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_kafka_source_lifecycle import consumer


@pytest.mark.asyncio
@pytest.mark.parametrize("tracing_enabled", [False, True])
async def test_binary_headers_preserve_record_and_valid_trace_metadata(tracing_enabled):
    c = consumer()
    extract = Mock(return_value=nullcontext(False))
    c._tracing = SimpleNamespace(extract=extract) if tracing_enabled else None
    c._endpoint = SimpleNamespace(id=1, environment=SimpleNamespace(config=SimpleNamespace(get_endpoint_config_by_id=lambda _: SimpleNamespace(tracing_enabled=False))))
    c._endpoint_request_inner = AsyncMock()
    record = SimpleNamespace(headers=[
        ("TRACEPARENT", b"\xff"),
        ("tracestate", b"vendor=first"),
        ("TRACESTATE", b"vendor=last"),
        ("BAGGAGE", b"tenant=test"),
        ("x-trace", None),
        ("binary-business-header", b"\xff\x00"),
    ])
    original = list(record.headers)
    await c._endpoint_request(record)
    c._endpoint_request_inner.assert_awaited_once_with(record)
    assert record.headers == original
    if tracing_enabled:
        extract.assert_called_once_with({"tracestate": "vendor=last", "baggage": "tenant=test"})
    else:
        extract.assert_not_called()


@pytest.mark.asyncio
async def test_only_invalid_trace_metadata_still_processes_record():
    c = consumer()
    extract = Mock()
    c._tracing = SimpleNamespace(extract=extract)
    c._endpoint = SimpleNamespace(id=1, environment=SimpleNamespace(config=SimpleNamespace(get_endpoint_config_by_id=lambda _: SimpleNamespace(tracing_enabled=False))))
    c._endpoint_request_inner = AsyncMock()
    record = SimpleNamespace(headers=[("traceparent", b"\xff"), ("x-trace", None)])
    await c._endpoint_request(record)
    extract.assert_not_called()
    c._endpoint_request_inner.assert_awaited_once_with(record)

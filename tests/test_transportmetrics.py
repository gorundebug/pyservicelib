import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from pyservicelib_gorundebug.datasink.http.aiohttpds import Requester
from pyservicelib_gorundebug.runtime.testmetrics import (
    TestMetrics as MetricsFixture,
)
from pyservicelib_gorundebug.runtime.transportmetrics import (
    TransportRequestMetrics,
)


def test_http_server_metrics_match_dashboard_contract() -> None:
    engine = MetricsFixture()
    metrics = TransportRequestMetrics.http_server(
        engine,
        method="POST",
        route="/v1/orders",
        host="0.0.0.0",
        port=9091,
    )

    request = metrics.start()
    metrics.finish(
        request,
        None,
        "201",
        request_body_size=512,
        response_body_size=128,
    )

    base = {
        "http_request_method": "POST",
        "http_route": "/v1/orders",
        "url_scheme": "http",
        "server_address": "0.0.0.0",
        "server_port": "9091",
    }
    assert engine.gauge("http_server_active_requests", base).value() == 0
    assert engine.histogram(
        "http_server_request_duration_seconds",
        {
            **base,
            "http_response_status_code": "201",
            "error_type": "",
        },
    ).count() == 1
    metric_labels = {
        **base,
        "http_response_status_code": "201",
        "error_type": "",
    }
    assert engine.histogram(
        "http_server_request_body_size_bytes",
        metric_labels,
    ).values() == [512.0]
    assert engine.histogram(
        "http_server_response_body_size_bytes",
        metric_labels,
    ).values() == [128.0]


def test_grpc_client_metrics_match_dashboard_contract() -> None:
    engine = MetricsFixture()
    metrics = TransportRequestMetrics.grpc_client(
        engine,
        method="InventoryService/ProcessOrderItem",
    )

    request = metrics.start()
    metrics.finish(request, RuntimeError("failed"))

    assert engine.histogram(
        "rpc_client_call_duration_seconds",
        {
            "rpc_system_name": "grpc",
            "rpc_method": "InventoryService/ProcessOrderItem",
            "rpc_response_status_code": "UNKNOWN",
        },
    ).count() == 1


async def test_http_client_metrics_match_dashboard_contract() -> None:
    async def reserve(_request: web.Request) -> web.Response:
        return web.Response(status=201, body=b"ok")

    server = TestServer(web.Application())
    server.app.router.add_post("/v1/reserve", reserve)
    await server.start_server()
    host = server.host
    port = server.port
    assert port is not None
    engine = MetricsFixture()
    url = str(server.make_url("/v1/reserve"))
    metrics = TransportRequestMetrics.http_client(
        engine,
        method="POST",
        route=url,
        server_address=host,
        server_port=port,
    )

    try:
        async with aiohttp.ClientSession() as session:
            requester = Requester(session).new_request(
                "POST", url, data=b"request-body",
            )
            request = metrics.start()
            async with await requester.execute() as response:
                response_body = await response.read()
                metrics.finish(
                    request,
                    None,
                    str(response.status),
                    request_body_size=requester.body_size,
                    response_body_size=len(response_body),
                )
    finally:
        await server.close()

    labels = {
        "http_request_method": "POST",
        "url_full": url,
        "server_address": host,
        "server_port": str(port),
        "network_protocol_version": "",
        "http_response_status_code": "201",
        "error_type": "",
    }
    assert engine.histogram(
        "http_client_request_duration_seconds", labels,
    ).count() == 1
    assert engine.histogram(
        "http_client_request_body_size_bytes", labels,
    ).values() == [12.0]
    assert engine.histogram(
        "http_client_response_body_size_bytes", labels,
    ).values() == [2.0]

from types import SimpleNamespace

import pytest
from aiohttp.test_utils import make_mocked_request

from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.runtime.statusweb import (
    _MDI_API,
    _MDI_CALL_MADE,
    _stream_icon_path,
    status_handler,
    vis_css_handler,
    vis_js_handler,
)


@pytest.mark.asyncio
async def test_status_page_uses_route_relative_assets_and_data() -> None:
    response = await status_handler(object(), make_mocked_request("GET", "/status"))

    assert response.status == 200
    assert response.content_type == "text/html"
    body = response.body.decode()
    assert "const statusBase = window.location.href" in body
    assert "statusBase + '/data'" in body
    assert "statusBase + '/vis.min.js'" in body
    assert "statusBase + '/vis.min.css'" in body
    assert "new vis.DataSet" in body
    assert "window.setTimeout(refreshNetwork, 1000)" in body


@pytest.mark.asyncio
async def test_status_assets_are_embedded_in_python_package() -> None:
    js = await vis_js_handler(
        object(), make_mocked_request("GET", "/status/vis.min.js")
    )
    css = await vis_css_handler(
        object(), make_mocked_request("GET", "/status/vis.min.css")
    )

    assert js.status == 200
    assert js.content_type == "application/javascript"
    assert len(js.body) > 100_000
    assert js.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    assert css.status == 200
    assert css.content_type == "text/css"
    assert len(css.body) > 10_000
    assert css.headers["Cache-Control"] == "public, max-age=31536000, immutable"


def test_http_and_grpc_endpoints_use_graph_designer_icons() -> None:
    config = SimpleNamespace(
        get_endpoint_config_by_id=lambda _: SimpleNamespace(id_data_connector=3),
        get_data_connector_by_id=lambda _: SimpleNamespace(type=DataConnectorType.HTTP),
    )
    app = SimpleNamespace(config=config)

    assert (
        _stream_icon_path(
            app, SimpleNamespace(type=TransformationType.Input, id_endpoint=7)
        )
        == _MDI_API
    )
    assert (
        _stream_icon_path(
            app, SimpleNamespace(type=TransformationType.Sink, id_endpoint=8)
        )
        == _MDI_CALL_MADE
    )

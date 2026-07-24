"""Generated HTTP transport adapter. DO NOT EDIT."""

from typing import Any

from pyservicelib_gorundebug.datasource.http.aiohttpds import (
    make_net_http_endpoint_consumer,
)
from pyservicelib_gorundebug.runtime.common import Consumer, TypedInputStream


def make_process_order_http_consumer(
    stream: TypedInputStream[Any, Any, Exception],
    handler: Any,
) -> Consumer[Any]:
    return make_net_http_endpoint_consumer(stream, handler)

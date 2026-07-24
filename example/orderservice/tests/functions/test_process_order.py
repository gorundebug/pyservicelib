from datetime import timedelta
from typing import Any, cast

import pytest

from orderservice.internal.functions import ProcessOrder
from orderservice.internal.functions.process_order import ProcessOrderState
from orderservice.internal.types import OrderState
from pyservicelib_gorundebug.datasource.http.aiohttpds import HandlerData
from pyservicelib_gorundebug.runtime.common import StreamContext


@pytest.mark.asyncio
async def test_process_order_request_lifecycle_sets_and_resets_deadline() -> None:
    function = ProcessOrder(timedelta(seconds=1))
    data = cast(HandlerData, object())
    sc = cast(StreamContext[Any, Any, Exception], object())

    returned_data, state = await function.begin_request(sc, data)
    assert returned_data is data

    await function.end_request(sc, None, state, data)


def test_process_order_correlates_by_order_id() -> None:
    function = ProcessOrder(timedelta(seconds=1))
    value = OrderState(order_id="order-1", status="CONFIRMED")
    state = cast(ProcessOrderState, object())
    sc = cast(StreamContext[Any, Any, Exception], object())

    assert function.get_message_id(sc, state, value) == "order-1"

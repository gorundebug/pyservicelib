from typing import Any, cast

import pytest

from example_model import OrderItemResult
from inventoryservice.internal.functions import ProcessOrderItem
from pyservicelib_gorundebug.runtime.common import StreamContext


@pytest.mark.asyncio
async def test_process_order_item_lifecycle_is_stateless() -> None:
    function = ProcessOrderItem()
    sc = cast(StreamContext[Any, Any, Exception], object())

    assert await function.begin_request(sc) is None
    function.eof(sc, None)
    await function.end_request(sc, None, None)


def test_process_order_item_correlates_by_item_id() -> None:
    value = OrderItemResult(
        "order-1", "item-1", "BOOK", 2, 2, True, "CONFIRMED", 12.5
    )

    assert (
        ProcessOrderItem().get_message_id(
            cast(StreamContext[Any, Any, Exception], object()),
            None,
            value,
        )
        == "item-1"
    )

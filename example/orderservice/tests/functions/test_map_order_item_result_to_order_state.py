import pytest

from example_model import OrderItemResult
from orderservice.internal.functions import MapOrderItemResultToOrderState

from .support import collector


@pytest.mark.asyncio
async def test_map_result_preserves_confirmed_item() -> None:
    result = OrderItemResult(
        "order-1", "item-1", "BOOK", 2, 2, True, "CONFIRMED", 12.5
    )
    values = []

    await MapOrderItemResultToOrderState().map(
        None,
        result,
        collector(values),
    )

    assert len(values) == 1
    assert values[0].order_id == "order-1"
    assert values[0].status == "CONFIRMED"
    assert values[0].confirmed_items == [result]

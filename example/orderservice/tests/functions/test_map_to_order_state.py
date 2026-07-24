import pytest

from orderservice.internal.functions import MapToOrderState
from orderservice.internal.types import Order

from .support import collector


@pytest.mark.asyncio
async def test_map_to_order_state_creates_timeout() -> None:
    order = Order(id="order-1", customer_id="customer-1", items=[])
    values = []

    await MapToOrderState().map(None, order, collector(values))

    assert len(values) == 1
    assert values[0].order_id == "order-1"
    assert values[0].status == "TIMED_OUT"
    assert values[0].confirmed_items == []

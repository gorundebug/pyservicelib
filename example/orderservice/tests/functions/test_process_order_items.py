import pytest

from example_model import OrderItem
from orderservice.internal.functions import ProcessOrderItems
from orderservice.internal.types import Order

from .support import collector


@pytest.mark.asyncio
async def test_process_order_items_expands_order() -> None:
    order = Order(
        id="order-1",
        customer_id="customer-1",
        items=[OrderItem("order-1", "item-1", "BOOK", 2)],
    )
    values: list[OrderItem] = []

    await ProcessOrderItems().flatmap(None, order, collector(values))

    assert values == [OrderItem("order-1", "item-1", "BOOK", 2)]

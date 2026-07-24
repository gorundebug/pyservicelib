import pytest

from example_model import OrderItem, OrderItemResult
from inventoryservice.internal.functions import GetInventoryItemData

from .support import collector


@pytest.mark.asyncio
async def test_inventory_routes_success_and_out_of_stock() -> None:
    function = GetInventoryItemData()
    results: list[OrderItemResult] = []
    rejected: list[OrderItemResult] = []

    await function.process(
        None,
        OrderItem("order-1", "item-1", "BOOK", 2),
        collector(results),
        collector(rejected),
    )
    await function.process(
        None,
        OrderItem("order-1", "item-2", "PHONE", 2),
        collector(results),
        collector(rejected),
    )

    assert results[0].reserved is True
    assert rejected[0].status == "OUT_OF_STOCK"

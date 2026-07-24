import asyncio

from example_model import OrderItem, OrderItemResult
from pyservicelib_gorundebug.runtime.common import Collect, Stream


class GetInventoryItemData:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._inventory: dict[str, tuple[int, float]] = {
            "BOOK": (10, 12.5),
            "PHONE": (1, 799.0),
        }

    async def process(
        self,
        stream: Stream,
        value: OrderItem,
        out: Collect[OrderItemResult],
        err_out: Collect[OrderItemResult],
    ) -> None:
        del stream
        async with self._lock:
            available, unit_price = self._inventory.get(value.sku, (0, 0.0))
            reserved = available >= value.quantity
            if reserved:
                self._inventory[value.sku] = (available - value.quantity, unit_price)

        result = OrderItemResult(
            order_id=value.order_id,
            item_id=value.item_id,
            sku=value.sku,
            requested_qty=value.quantity,
            available_qty=value.quantity if reserved else available,
            reserved=reserved,
            status="CONFIRMED" if reserved else "OUT_OF_STOCK",
            unit_price=unit_price,
        )
        await (out if reserved else err_out).out(result)

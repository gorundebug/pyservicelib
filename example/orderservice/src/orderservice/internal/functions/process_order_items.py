from example_model import OrderItem
from pyservicelib_gorundebug.runtime.common import Collect, Stream

from ..types import Order


class ProcessOrderItems:
    async def flatmap(
        self,
        stream: Stream,
        value: Order,
        out: Collect[OrderItem],
    ) -> None:
        del stream
        for item in value.items:
            await out.out(
                OrderItem(
                    order_id=value.id,
                    item_id=item.item_id,
                    sku=item.sku,
                    quantity=item.quantity,
                )
            )

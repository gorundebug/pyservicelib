from example_model import OrderItemResult
from pyservicelib_gorundebug.runtime.common import Collect, Stream

from ..types import OrderState


class MapOrderItemResultToOrderState:
    async def map(
        self,
        stream: Stream,
        value: OrderItemResult,
        out: Collect[OrderState],
    ) -> None:
        del stream
        await out.out(
            OrderState(
                order_id=value.order_id,
                status="CONFIRMED" if value.reserved else "PARTIALLY_CONFIRMED",
                confirmed_items=[value],
            )
        )

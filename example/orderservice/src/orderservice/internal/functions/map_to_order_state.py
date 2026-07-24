from pyservicelib_gorundebug.runtime.common import Collect, Stream

from ..types import Order, OrderState


class MapToOrderState:
    async def map(
        self,
        stream: Stream,
        value: Order,
        out: Collect[OrderState],
    ) -> None:
        del stream
        await out.out(OrderState(order_id=value.id, status="TIMED_OUT"))

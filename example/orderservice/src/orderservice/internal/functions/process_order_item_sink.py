from typing import Any

from example_model import OrderItem, OrderItemResult
from inventory_service_api.generated import processorderitem_pb2
from pyservicelib_gorundebug.datasink.grpc.grpcds import ResultContext, Sender
from pyservicelib_gorundebug.runtime.common import SinkStreamContext


class ProcessOrderItemSink:
    async def begin_request(
        self,
        sc: SinkStreamContext[OrderItem, OrderItemResult, Exception],
    ) -> None:
        del sc
        return None

    async def consume_message(
        self,
        sc: SinkStreamContext[OrderItem, OrderItemResult, Exception],
        handler_state: None,
        value: OrderItem,
        sender: Sender[Any],
        result_ctx: ResultContext,
    ) -> None:
        del sc, handler_state, result_ctx
        await sender.send(
            processorderitem_pb2.ProcessOrderItemRequest(
                order_id=value.order_id,
                item_id=value.item_id,
                sku=value.sku,
                quantity=value.quantity,
            )
        )

    async def handle_response(
        self,
        sc: SinkStreamContext[OrderItem, OrderItemResult, Exception],
        handler_state: None,
        response: Any,
    ) -> None:
        del handler_state
        await sc.collect(
            OrderItemResult(
                order_id=response.order_id,
                item_id=response.item_id,
                sku=response.sku,
                requested_qty=response.requested_qty,
                available_qty=response.available_qty,
                reserved=response.reserved,
                status=response.status,
                unit_price=response.unit_price,
            )
        )

    async def end_request(
        self,
        sc: SinkStreamContext[OrderItem, OrderItemResult, Exception],
        err: Exception | None,
        handler_state: None,
    ) -> None:
        del sc, err, handler_state

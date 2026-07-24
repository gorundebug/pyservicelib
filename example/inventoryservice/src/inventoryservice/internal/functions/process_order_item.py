import asyncio
from typing import Any

from example_model import OrderItem, OrderItemResult
from inventory_service_api.generated import processorderitem_pb2
from pyservicelib_gorundebug.datasource.grpc.grpcds import ResultContext, Sender
from pyservicelib_gorundebug.runtime.common import StreamContext


class ProcessOrderItem:
    async def begin_request(
        self,
        sc: StreamContext[OrderItem, OrderItemResult, Exception],
    ) -> None:
        del sc
        return None

    async def consume_message(
        self,
        sc: StreamContext[OrderItem, OrderItemResult, Exception],
        handler_state: None,
        request: Any,
        result_ctx: ResultContext[None, OrderItem, Any, OrderItemResult, Exception],
        sender: Sender[Any],
    ) -> None:
        del handler_state
        item = OrderItem(
            order_id=request.order_id,
            item_id=request.item_id,
            sku=request.sku,
            quantity=request.quantity,
        )

        def on_result(
            sc: StreamContext[OrderItem, OrderItemResult, Exception],
            handler_state: None,
            value: OrderItemResult,
            sender: Sender[Any],
        ) -> bool:
            del sc, handler_state
            response = processorderitem_pb2.ProcessOrderItemResponse(
                order_id=value.order_id,
                item_id=value.item_id,
                sku=value.sku,
                requested_qty=value.requested_qty,
                available_qty=value.available_qty,
                reserved=value.reserved,
                status=value.status,
                unit_price=value.unit_price,
            )

            async def send() -> None:
                await sender.send(response)
                result_ctx.done()

            asyncio.create_task(send())
            return True

        result_ctx.set_result_callback(item.item_id, on_result)
        await sc.collect(item)

    def get_message_id(
        self,
        sc: StreamContext[OrderItem, OrderItemResult, Exception],
        handler_state: None,
        value: OrderItemResult,
    ) -> str:
        del sc, handler_state
        return value.item_id

    def eof(
        self,
        sc: StreamContext[OrderItem, OrderItemResult, Exception],
        handler_state: None,
    ) -> None:
        del sc, handler_state

    async def end_request(
        self,
        sc: StreamContext[OrderItem, OrderItemResult, Exception],
        err: Exception | None,
        handler_state: None,
    ) -> None:
        del sc, err, handler_state

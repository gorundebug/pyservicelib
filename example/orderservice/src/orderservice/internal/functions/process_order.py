from contextvars import Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from aiohttp import web
from example_model import OrderItem, OrderItemResult
from order_service_api import ConfirmedOrderItem, ProcessOrderRequest, ProcessOrderResponse
from pyservicelib_gorundebug.datasource.http.aiohttpds import HandlerData, ResultContext
from pyservicelib_gorundebug.runtime.common import StreamContext
from pyservicelib_gorundebug.runtime.context.request import request_deadline

from ..types import Order, OrderState


@dataclass(slots=True)
class ProcessOrderState:
    deadline_token: Token[Any]
    expected_items: int = 0
    results: list[OrderItemResult] = field(default_factory=list)
    response_sent: bool = False


class ProcessOrder:
    def __init__(self, timeout: timedelta):
        self._timeout = timeout

    async def begin_request(
        self,
        sc: StreamContext[Order, OrderState, Exception],
        data: HandlerData,
    ) -> tuple[HandlerData, ProcessOrderState]:
        del sc
        deadline = datetime.now(timezone.utc) + self._timeout
        return data, ProcessOrderState(request_deadline.set(deadline))

    async def consume_message(
        self,
        sc: StreamContext[Order, OrderState, Exception],
        handler_state: ProcessOrderState,
        data: HandlerData,
        result_ctx: ResultContext[ProcessOrderState, Order, OrderState, Exception],
    ) -> None:
        try:
            request = ProcessOrderRequest.model_validate(await data.request.json())
        except Exception as error:
            data.set_response(web.json_response({"error": "invalid request", "detail": str(error)}, status=400))
            raise

        order_id = str(uuid4())
        handler_state.expected_items = len(request.items)

        def on_result(
            sc: StreamContext[Order, OrderState, Exception],
            handler_state: ProcessOrderState,
            value: OrderState,
            data: HandlerData,
        ) -> bool:
            del sc
            if handler_state.response_sent:
                return True
            if value.status != "TIMED_OUT":
                handler_state.results.extend(value.confirmed_items)
                if len(handler_state.results) < handler_state.expected_items:
                    return False

            handler_state.response_sent = True
            status = value.status
            if status != "TIMED_OUT":
                status = (
                    "CONFIRMED"
                    if all(item.reserved for item in handler_state.results)
                    else "PARTIALLY_CONFIRMED"
                )
            response = ProcessOrderResponse(
                order_id=order_id,
                status=status,
                confirmed_items=[
                    _confirmed_item(item) for item in handler_state.results
                ],
                total_amount=sum(
                    item.unit_price * item.requested_qty
                    for item in handler_state.results
                ),
                processed_at=datetime.now(timezone.utc),
            )
            data.set_response(
                web.json_response(response.model_dump(mode="json", by_alias=True))
            )
            result_ctx.done()
            return True

        result_ctx.set_result_callback(order_id, on_result)
        await sc.collect(
            Order(
                id=order_id,
                customer_id=request.customer_id or "",
                items=[
                    OrderItem(
                        order_id=order_id,
                        item_id=item.item_id,
                        sku=item.sku,
                        quantity=item.quantity,
                    )
                    for item in request.items
                ],
            )
        )

    def get_message_id(
        self,
        sc: StreamContext[Order, OrderState, Exception],
        handler_state: ProcessOrderState,
        value: OrderState,
    ) -> str:
        del sc, handler_state
        return value.order_id

    async def end_request(
        self,
        sc: StreamContext[Order, OrderState, Exception],
        err: Exception | None,
        handler_state: ProcessOrderState,
        data: HandlerData,
    ) -> None:
        del sc, err, data
        request_deadline.reset(handler_state.deadline_token)


def _confirmed_item(value: OrderItemResult) -> ConfirmedOrderItem:
    return ConfirmedOrderItem(
        order_id=value.order_id,
        item_id=value.item_id,
        sku=value.sku,
        requested_qty=value.requested_qty,
        available_qty=value.available_qty,
        reserved=value.reserved,
        status=value.status,
        unit_price=value.unit_price,
    )

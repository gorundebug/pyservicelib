from typing import Any, cast

import pytest

from example_model import OrderItem
from inventory_service_api.generated import processorderitem_pb2
from orderservice.internal.functions import ProcessOrderItemSink
from pyservicelib_gorundebug.datasink.grpc.grpcds import Sender
from pyservicelib_gorundebug.runtime.common import SinkStreamContext


class RecordingSender(Sender[Any]):
    def __init__(self) -> None:
        self.values: list[Any] = []

    async def send(self, value: Any) -> None:
        self.values.append(value)


@pytest.mark.asyncio
async def test_process_order_item_sink_maps_request() -> None:
    sender = RecordingSender()
    item = OrderItem("order-1", "item-1", "BOOK", 2)

    await ProcessOrderItemSink().consume_message(
        cast(SinkStreamContext[Any, Any, Exception], object()),
        None,
        item,
        sender,
        cast(Any, object()),
    )

    assert sender.values == [
        processorderitem_pb2.ProcessOrderItemRequest(
            order_id="order-1",
            item_id="item-1",
            sku="BOOK",
            quantity=2,
        )
    ]

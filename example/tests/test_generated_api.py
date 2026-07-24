from inventory_service_api.generated import processorderitem_pb2
from order_service_api import ProcessOrderRequest


def test_generated_transport_models_round_trip() -> None:
    request = processorderitem_pb2.ProcessOrderItemRequest(
        order_id="order-1",
        item_id="item-1",
        sku="BOOK",
        quantity=2,
    )
    assert processorderitem_pb2.ProcessOrderItemRequest.FromString(
        request.SerializeToString()
    ) == request

    http_request = ProcessOrderRequest.model_validate(
        {"customerId": "customer-1", "items": [{"itemId": "item-1", "sku": "BOOK", "quantity": 2}]}
    )
    assert http_request.items[0].item_id == "item-1"

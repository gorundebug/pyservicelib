from dataclasses import dataclass


@dataclass(slots=True)
class OrderItem:
    order_id: str
    item_id: str
    sku: str
    quantity: int


@dataclass(slots=True)
class OrderItemResult:
    order_id: str
    item_id: str
    sku: str
    requested_qty: int
    available_qty: int
    reserved: bool
    status: str
    unit_price: float

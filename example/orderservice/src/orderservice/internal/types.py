from dataclasses import dataclass, field
from datetime import datetime, timezone

from example_model import OrderItem, OrderItemResult


@dataclass(slots=True)
class Order:
    id: str
    customer_id: str
    items: list[OrderItem]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class OrderState:
    order_id: str
    status: str
    confirmed_items: list[OrderItemResult] = field(default_factory=list)
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

# Code generated from processorder.yaml. DO NOT EDIT.

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class ProcessOrderItem(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        populate_by_name=True,
        alias_generator=to_camel,
    )
    item_id: str
    sku: str
    quantity: int = Field(..., ge=1)


class ConfirmedOrderItem(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        populate_by_name=True,
        alias_generator=to_camel,
    )
    order_id: str
    item_id: str
    sku: str
    requested_qty: int
    available_qty: int
    reserved: bool
    status: str
    unit_price: float


class ProcessOrderRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        populate_by_name=True,
        alias_generator=to_camel,
    )
    customer_id: str | None = None
    items: list[ProcessOrderItem] = Field(..., min_length=1)


class ProcessOrderResponse(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        populate_by_name=True,
        alias_generator=to_camel,
    )
    order_id: str
    status: str
    confirmed_items: list[ConfirmedOrderItem]
    total_amount: float
    processed_at: AwareDatetime

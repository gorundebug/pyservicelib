from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ProcessOrderItemRequest(_message.Message):
    __slots__ = ("order_id", "item_id", "sku", "quantity")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    item_id: str
    sku: str
    quantity: int
    def __init__(self, order_id: _Optional[str] = ..., item_id: _Optional[str] = ..., sku: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class ProcessOrderItemResponse(_message.Message):
    __slots__ = ("order_id", "item_id", "sku", "requested_qty", "available_qty", "reserved", "status", "unit_price")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_QTY_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_QTY_FIELD_NUMBER: _ClassVar[int]
    RESERVED_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    item_id: str
    sku: str
    requested_qty: int
    available_qty: int
    reserved: bool
    status: str
    unit_price: float
    def __init__(self, order_id: _Optional[str] = ..., item_id: _Optional[str] = ..., sku: _Optional[str] = ..., requested_qty: _Optional[int] = ..., available_qty: _Optional[int] = ..., reserved: _Optional[bool] = ..., status: _Optional[str] = ..., unit_price: _Optional[float] = ...) -> None: ...

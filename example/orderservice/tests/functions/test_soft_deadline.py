from datetime import datetime, timedelta, timezone

import pytest

from orderservice.internal.functions import SoftDeadline
from orderservice.internal.types import Order
from pyservicelib_gorundebug.runtime.context.request import request_deadline


@pytest.mark.asyncio
async def test_soft_deadline_subtracts_safety_margin() -> None:
    function = SoftDeadline(timedelta(milliseconds=100))
    order = Order(id="order-1", customer_id="customer-1", items=[])
    token = request_deadline.set(
        datetime.now(timezone.utc) + timedelta(seconds=1)
    )
    try:
        duration = await function.duration(None, order)
    finally:
        request_deadline.reset(token)

    assert timedelta(milliseconds=700) < duration < timedelta(seconds=1)

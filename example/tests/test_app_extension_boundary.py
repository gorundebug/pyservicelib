from inventoryservice.internal.app.service import Service as InventoryService
from inventoryservice.internal.app.service_generated import (
    GeneratedService as GeneratedInventoryService,
)
from orderservice.internal.app.service import Service as OrderService
from orderservice.internal.app.service_generated import (
    GeneratedService as GeneratedOrderService,
)
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp


def test_user_services_extend_generated_services() -> None:
    assert issubclass(OrderService, GeneratedOrderService)
    assert issubclass(InventoryService, GeneratedInventoryService)


def test_user_hooks_do_not_replace_runtime_protocol() -> None:
    assert "service_init" not in OrderService.__dict__
    assert "service_init" not in InventoryService.__dict__
    assert OrderService.service_init is ServiceApp.service_init
    assert InventoryService.service_init is ServiceApp.service_init

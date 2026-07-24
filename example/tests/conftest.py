import sys
from pathlib import Path


EXAMPLE = Path(__file__).parents[1]
for package in (
    "model",
    "order_service_api",
    "inventory_service_api",
    "orderservice",
    "inventoryservice",
):
    sys.path.insert(0, str(EXAMPLE / package / "src"))

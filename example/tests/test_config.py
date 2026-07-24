from pathlib import Path

import yaml

from inventoryservice.internal.config import Config as InventoryConfig
from orderservice.internal.config import Config as OrderConfig


ROOT = Path(__file__).parents[1]


def _load(path: Path, config_type):
    return config_type.from_dict(yaml.safe_load(path.read_text()))


def test_order_config_matches_graph() -> None:
    config = _load(ROOT / "orderservice/config/config.yaml", OrderConfig)
    assert config is not None
    assert len(config.streams) == 8
    assert len(config.links) == 9
    assert config.named.streams.process_order.id_endpoint == 1
    assert config.named.endpoints.process_order.id == 1
    assert config.named.data_connectors.inventory_service_api.id == 1


def test_inventory_config_matches_graph() -> None:
    config = _load(ROOT / "inventoryservice/config/config.yaml", InventoryConfig)
    assert config is not None
    assert len(config.streams) == 3
    assert len(config.links) == 3
    assert config.named.streams.process_inventory_item.id_endpoint == 1
    assert config.named.endpoints.process_order_item.id == 1

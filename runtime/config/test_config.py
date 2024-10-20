#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Any, Dict
import yaml
from pathlib import Path

from pyservicelib.runtime.config import ServiceAppConfig, replace_placeholders

def test_load_config():
    current_dir = Path(__file__).parent

    with open(current_dir / 'config_test.yaml', 'r') as file:
        config_data: Dict[str, Any] = yaml.safe_load(file)

    with open(current_dir / 'values_test.yaml', 'r') as file:
        values_data: Dict[str, Any] = yaml.safe_load(file)

    result_config: Dict[str, Any] = replace_placeholders(config_data, values_data)
    ServiceAppConfig.from_dict(result_config)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""Unit tests for the StreamAppConfig class."""

import unittest
from typing import Any, Dict
import yaml

from pyservicelib.runtime.config import StreamAppConfig, replace_placeholders

class ConfigLoadTestCase(unittest.TestCase):
    """Tests for the StreamAppConfig class."""

    def test_load_config(self):
        """Test load of StreamAppConfig from file."""
        with open('config_test.yaml', 'r') as file:
            config_data: Dict[str, Any] = yaml.safe_load(file)

        with open('values_test.yaml', 'r') as file:
            values_data: Dict[str, Any] = yaml.safe_load(file)

        result_config: Dict[str, Any] = replace_placeholders(config_data, values_data)
        StreamAppConfig.from_dict(result_config)

if __name__ == '__main__':
    unittest.main()

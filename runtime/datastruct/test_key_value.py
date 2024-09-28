#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""Unit tests for the KeyValue class from pyservicelib.runtime.datastruct.key_value."""

import unittest

from pyservicelib.runtime.datastruct import KeyValue

class KeyValueTestCase(unittest.TestCase):
    """Tests for the KeyValue class."""

    def test_key_value(self):
        """Test initialization of KeyValue with integer key and string value."""
        kv = KeyValue[int, str](5, "test")
        self.assertEqual(kv.key, 5)
        self.assertEqual(kv.value, "test")

if __name__ == '__main__':
    unittest.main()

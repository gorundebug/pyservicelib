#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""Unit tests for the KeyValue class from pyservicelib.runtime.datastruct.key_value."""

import unittest

from pyservicelib.runtime.stream import StreamBase

class StreamBaseTypeCase(unittest.TestCase):
    """Tests for the StreamBase class."""

    def test_stream_base_type_name(self):
        """Test generic type name."""
        stream = StreamBase[int](None, None) #pyright: ignore
        self.assertEqual(stream.type_name, "int")

if __name__ == '__main__':
    unittest.main()

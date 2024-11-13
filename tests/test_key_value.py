#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib_gorundebug.runtime.datastruct import KeyValue

def test_key_value():
    kv = KeyValue[int, str](5, "test")
    assert kv.key == 5
    assert kv.value == "test"

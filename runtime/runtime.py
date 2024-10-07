#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#  file for details.
#
from typing import cast
from pyservicelib.runtime.environment import ServiceExecutionRuntime
from pyservicelib.runtime.serde import StreamSerde, SerdeTypeHelper


class RuntimeTypeHelpers[T]:
    _runtime: ServiceExecutionRuntime

    def __init__(self, runtime: ServiceExecutionRuntime):
        self._runtime = runtime

    def get_registered_serde(self) -> StreamSerde[T]:
        return cast(StreamSerde[T], self._runtime.get_registered_serde(SerdeTypeHelper[T]().get_type()))

    def make_serde(self) -> StreamSerde[T]:
        return self.get_registered_serde()
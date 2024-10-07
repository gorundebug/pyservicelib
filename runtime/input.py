#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime.environment import ServiceExecutionEnvironment, TypedConsumedStream
from pyservicelib.runtime.runtime import RuntimeTypeHelpers

class InputStream[T](TypedConsumedStream[T]):
    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        super().__init__(name, RuntimeTypeHelpers[T](env.runtime).make_serde(), env)

    @property
    def endpoint_id(self):
        return self.config.id_endpoint

    def consume(self, value: T) -> None:
        if self._caller is not None:
            self._caller.consume(value)







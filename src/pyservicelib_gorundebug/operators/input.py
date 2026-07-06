#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from ..runtime.common import ServiceExecutionEnvironment, TypedInputStream, RuntimeHelpers
from ..runtime.config.stream_types import InputStreamConfig


class InputStream[T](TypedInputStream[T]):
    _id_endpoint: int

    def __init__(self, cfg: InputStreamConfig, env: ServiceExecutionEnvironment):
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[T](env).make_stream_serde(type_name=cfg.value_type))
        self._id_endpoint = cfg.id_endpoint

    @property
    def endpoint_id(self) -> int:
        return self._id_endpoint

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

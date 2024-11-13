#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from .common import ServiceExecutionEnvironment, TypedInputStream, RuntimeHelpers


class InputStream[T](TypedInputStream[T]):
    _id_endpoint: int

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"InputStream configuration named '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the InputStream with name '{name}' is not defined")
        if cfg.id_endpoint is None:
            raise ValueError(f"The endpoint id of the InputStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[T](env).make_stream_serde(type_name=cfg.value_type))
        self._id_endpoint = cfg.id_endpoint

    @property
    def endpoint_id(self) -> int:
        return self._id_endpoint

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from pyservicelib.runtime import ServiceExecutionEnvironment, TypedConsumedStream, RuntimeHelpers


class InputStream[T](TypedConsumedStream[T]):
    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"InputStream configuration named '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the InputStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id,
                         serde=RuntimeHelpers[T](env).make_serde(type_name=cfg.value_type),
                         env=env)

    @property
    def endpoint_id(self):
        return self.config.id_endpoint

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

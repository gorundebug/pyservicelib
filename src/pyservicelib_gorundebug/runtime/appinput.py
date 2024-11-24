#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from .common import ServiceExecutionEnvironment, TypedConsumedStream, RuntimeHelpers


class AppInputStream[T](TypedConsumedStream[T]):

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"AppInputStream configuration named '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the AppInputStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[T](env).make_stream_serde(type_name=cfg.value_type))

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

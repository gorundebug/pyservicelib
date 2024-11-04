#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Optional

from pyservicelib.runtime import ServiceExecutionEnvironment, TypedLinkStream, TypedStream, StreamConsumer, Stream


class LinkStream[T](TypedLinkStream[T]):

    _source: Optional[TypedStream[T]]
    _consumer: Optional[StreamConsumer[T]]

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"LinkStream configuration names '{name}' not found")

        super().__init__(stream_id=cfg.id,
                         env=env)

        self._source = None
        self._consumer = None

    async def consume(self, value: T) -> None:
        if self._consumer is not None:
            await self._consumer.consume(value)

    @property
    def consumer(self) -> Optional[StreamConsumer[T]]:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[T]):
        self._consumer = value

    def set_source(self, stream: TypedStream[T]):
        self._source = stream
        self._serde = stream.serde
        stream.consumer = self


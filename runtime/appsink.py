#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from pyservicelib.runtime import TypedStream, Consumer, StreamConsumer, Stream
from pyservicelib.runtime.serde import TypeHelpers

class AppSinkStream[T](Stream, StreamConsumer[T]):
    _source: TypedStream[T]
    _consumer: Consumer[T]

    def __init__(self, name: str, stream: TypedStream[T], consumer: Consumer[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"AppSinkStream configuration names '{name}' not found")

        super().__init__(cfg=cfg,
                         env=stream.environment)

        self._source = stream
        self._consumer = consumer
        stream.consumer = self

    async def consume(self, value: T) -> None:
        await self._consumer.consume(value)

    @property
    def stream(self) -> Stream:
        return self

    @property
    def type_name(self) -> str:
        genetic_type = TypeHelpers[T]().get_type() #pyright: ignore
        return genetic_type.__name__


#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Optional

from pyservicelib.runtime import TypedSinkStream, TypedStream, StreamConsumer

class SinkStream[T](TypedSinkStream[T]):
    _source: TypedStream[T]
    _consumer: Optional[StreamConsumer[T]]
    _endpoint_id: int

    def __init__(self, name: str, stream: TypedStream[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"SinkStream configuration names '{name}' not found")
        if cfg.id_endpoint is None:
            raise ValueError(f"endpoint_id is None for SinkStream '{cfg.name}'")

        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)

        self._source = stream
        self._endpoint_id = cfg.id_endpoint
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

    @property
    def endpoint_id(self) -> int:
        return self._endpoint_id


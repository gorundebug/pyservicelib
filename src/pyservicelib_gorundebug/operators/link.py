#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional

from ..runtime.common import ServiceExecutionEnvironment, TypedLinkStream, TypedStream, StreamConsumer
from ..runtime.config.stream_types import CycleLinkStreamConfig
from ..runtime.environment.tracing import Tracer, start_stream_span


class LinkStream[T](TypedLinkStream[T]):
    _source: Optional[TypedStream[T]]
    _consumer: Optional[StreamConsumer[T]]
    _tracer: Optional[Tracer]

    def __init__(self, cfg: CycleLinkStreamConfig, env: ServiceExecutionEnvironment):
        super().__init__(stream_id=cfg.id, env=env)
        self._source = None
        self._consumer = None
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None

    async def consume(self, value: T) -> None:
        _, span = start_stream_span(self._tracer, "stream.link", self)
        try:
            with span.scoped():
                if self._consumer is not None:
                    await self._consumer.consume(value)
        finally:
            span.end()

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

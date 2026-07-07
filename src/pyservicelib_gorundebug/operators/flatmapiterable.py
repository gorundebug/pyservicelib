#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from collections.abc import Iterable
from typing import get_origin, cast, Optional

from ..runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from ..runtime.config.stream_types import FlatMapIterableStreamConfig
from ..runtime.environment.tracing import Tracer, start_span, string_attr, sampling_enabled


class FlatMapIterableStream[T: Iterable, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _element_type: type
    _tracer: Optional[Tracer]

    def __init__(self, cfg: FlatMapIterableStreamConfig, stream: TypedStream[T]):
        env = stream.environment
        super().__init__(stream_id=cfg.id, env=env,
                         serde=RuntimeHelpers[R](env).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None
        stream.consumer = self

    def build(self):
        genetic_type = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type

        if not issubclass(orig_type, Iterable):
            raise TypeError(f"FlatMapIterable type '{orig_type.__name__}' isn't iterable.")

        genetic_type = self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
        self._element_type = get_origin(genetic_type)
        if self._element_type is None:
            self._element_type = genetic_type

    async def consume(self, value: T) -> None:
        _, span = start_span(self._tracer if sampling_enabled() else None, "stream.flatmap_iterable",
                             string_attr("stream", self.name))
        try:
            with span.scoped():
                if self._caller is not None:
                    for elem in value:
                        if isinstance(elem, self._element_type):
                            await self._caller.consume(cast(R, elem))
                        else:
                            raise ValueError(
                                f"Element in the consume method for the FlatMapIterable stream '{self.name}' "
                                f"has an invalid type: Element Type: {type(elem).__name__}, "
                                f"Required Type: {self._element_type.__name__}"
                            )
        finally:
            span.end()

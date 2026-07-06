#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from collections.abc import Iterable
from typing import get_origin, cast

from ..runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers
from ..runtime.config.stream_types import FlatMapIterableStreamConfig


class FlatMapIterableStream[T: Iterable, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _element_type: type

    def __init__(self, cfg: FlatMapIterableStreamConfig, stream: TypedStream[T]):
        super().__init__(stream_id=cfg.id, env=stream.environment,
                         serde=RuntimeHelpers[R](stream.environment).make_stream_serde(type_name=cfg.value_type))
        self._source = stream
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

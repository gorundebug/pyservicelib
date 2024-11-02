#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from collections.abc import Iterable
from typing import get_origin

from pyservicelib.runtime.common import TypedStreamSerde, Collector
from pyservicelib.runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeHelpers


class FlatMapIterableStream[T: Iterable, R](TypedTransformConsumedStream[T, R]):
    _source: TypedStream[T]
    _serdeIn: TypedStreamSerde[T]
    _element_type: type

    def __init__(self, name: str, stream: TypedStream[T]):
        genetic_type = self.__orig_class__.__args__[0] #pyright: ignore
        orig_type = get_origin(genetic_type)
        if orig_type is None:
            orig_type = genetic_type

        if not issubclass(orig_type, Iterable):
            raise TypeError(f"FlatMapIterable type '{orig_type.__name__}' isn't iterable.")

        genetic_type = self.__orig_class__.__args__[1] #pyright: ignore
        self._element_type = get_origin(genetic_type)
        if self._element_type is None:
            self._element_type = genetic_type

        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"FlatMapIterableStream configuration names '{name}' not found")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the FlatMapIterableStream with name '{name}' is not defined")

        super().__init__(cfg=cfg,
                         serde=RuntimeHelpers[R](stream.environment).make_serde(type_name=cfg.value_type),
                         env=stream.environment)
        self._source = stream
        self._serdeIn = stream.serde
        stream.consumer = self

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            for elem in value:
                if isinstance(elem, self._element_type):
                    await self._caller.consume(elem)
                else:
                    raise ValueError(f"""Element in the consume method for the FlatMapIterable stream '{self.name}'
has an invalid type: Element Type: {type(elem).__name__}, Required Type: {self._element_type.__name__}""")



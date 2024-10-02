#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional
from pyservicelib.runtime.environment import Stream, StreamExecutionRuntime, Caller, TypedStreamConsumer
from pyservicelib.runtime.serde import StreamSerde
from pyservicelib.runtime.config import StreamConfig

class StreamBase[T](Stream):
    _config: StreamConfig
    _runtime: StreamExecutionRuntime

    def __init__(self, name: str, runtime: StreamExecutionRuntime):
        cfg = runtime.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"Stream configuration with name '{name}' not found")
        self._config = cfg
        self._runtime = runtime

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def transformation_name(self) -> str:
        return self.transformation_name

    @property
    def type_name(self) -> str:
        genetic_type = self.__orig_class__.__args__[0] #pyright: ignore
        return genetic_type.__name__

    @property
    def id(self) -> int:
        return self._config.id

    @property
    def config(self) -> StreamConfig:
        return self._config

    @property
    def runtime(self) -> StreamExecutionRuntime:
        return self._runtime


class ConsumedStream[T](StreamBase[T]):
    _caller: Optional[Caller[T]]
    _serde:  Optional[StreamSerde[T]]
    _consumer: Optional[TypedStreamConsumer[T]]

    def __init__(self, name: str, runtime: StreamExecutionRuntime):
        super().__init__(name, runtime)
        self._caller = None
        self._serde = None
        self._consumer = None
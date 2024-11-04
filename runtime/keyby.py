#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Hashable

from pyservicelib.runtime.common import StreamFunction
from pyservicelib.runtime.common import TypedStream, TypedTransformConsumedStream, RuntimeKeyValueHelpers
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.functions import KeyByFunction


class KeyByFunctionContext[T, K: Hashable, V](StreamFunction[KeyValue[K, V]]):
    _fn: KeyByFunction[T, K, V]

    def __init__(self, context: TypedStream[KeyValue[K, V]], fn: KeyByFunction[T, K, V]):
        super().__init__(context)
        self._fn = fn

    async def call(self, value: T) -> KeyValue[K, V]:
        self.before_call()
        result = await self._fn.key_by(self._context, value)
        self.after_call()
        return result


class KeyByStream[T, K, V](TypedTransformConsumedStream[T, KeyValue[K, V]]):
    _source: TypedStream[T]
    _f: KeyByFunctionContext[T, K, V]

    def __init__(self, name: str, stream: TypedStream[T], fn: KeyByFunction[T, K, V]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"KeyByStream configuration names '{name}' not found")
        if cfg.key_type is None:
            raise ValueError(f"The key type of the KeyByStream with name '{name}' is not defined")
        if cfg.value_type is None:
            raise ValueError(f"The value type of the KeyByStream with name '{name}' is not defined")

        super().__init__(stream_id=cfg.id, env=stream.environment,
                         serde=RuntimeKeyValueHelpers[K, V](stream.environment).
                         make_key_value_stream_serde(key_type_name=cfg.key_type,
                                              value_type_name=cfg.value_type))
        self._source = stream
        self._f = KeyByFunctionContext[T, K, V](self, fn)
        stream.consumer = self

    async def consume(self, value: T) -> None:
        kv = await self._f.call(value)
        if self._caller is not None:
            await self._caller.consume(kv)

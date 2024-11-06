#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Optional, Hashable

from pyservicelib.api.models.transformation_type import TransformationType
from pyservicelib.runtime import TypedStream, TypedConsumedStream, TypedSplitStream
from pyservicelib.runtime import Stream, ServiceExecutionEnvironment, RuntimeHelpers
from pyservicelib.runtime.common import RuntimeKeyValueHelpers, BinaryConsumer, BinaryKVConsumer
from pyservicelib.runtime.datastruct import KeyValue
from pyservicelib.runtime.serde import TypedStreamSerde, BytesBuffer, TypedStreamKeyValueSerde


class SplitLink[T](TypedConsumedStream[T]):
    _split_stream: "SplitStreamBase[T]"
    _index: int

    def __init__(self, split_stream: "SplitStreamBase[T]", index: int):
        super().__init__(split_stream.id, split_stream.environment, split_stream.serde)
        self._split_stream = split_stream
        self._index = index

    @property
    def name(self) -> str:
        return f"{self._split_stream.name}SplitLink{self._index}"

    @property
    def consumers(self) -> list[Stream]:
        return self._split_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._split_stream

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)


class SplitStreamBase[T](TypedSplitStream[T]):
    _links: list[SplitLink[T]]
    _source: Optional[TypedStream[T]]

    def __init__(self, name: str,
                 env: ServiceExecutionEnvironment,
                 serde: TypedStreamSerde[T],
                 stream: Optional[TypedStream[T]] = None):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"SplitStream configuration names '{name}' not found")

        super().__init__(stream_id=cfg.id, env=env, serde=serde)
        self._source = stream
        self._links = []
        if stream is not None:
            stream.consumer = self

    async def consume(self, value: T) -> None:
        for link in self._links:
            await link.consume(value)

    @property
    def consumers(self) -> list[Stream]:
        consumers: list[Stream] = []
        for link in self._links:
            if link.consumer is None:
                raise ValueError(f"SplitStream '{self.name}' does not have a consumer for all split streams")
            consumers.append(link.consumer.stream)
        return consumers

    def build(self):
        for i, link in enumerate(self._links):
            if link.consumer is None:
                raise ValueError(f"SplitStream '{self.name}' does not have a consumer for the link with index {i}")

    def add_stream(self) -> TypedConsumedStream[T]:
        index = len(self._links)
        link = SplitLink(self, index)
        self._links.append(link)
        return link


class SplitStream[T](SplitStreamBase[T]):
    def __init__(self, name: str, stream: TypedStream[T]):
        super().__init__(name=name, env=stream.environment, serde=stream.serde, stream=stream)


class TypedBinarySplitStream[T](SplitStreamBase[T], BinaryConsumer[T]):

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"SplitStream configuration names '{name}' not found")

        stream_cfg = cfg
        while stream_cfg.id_source != 0:
            stream_cfg = env.config.get_stream_config_by_id(stream_cfg.id_source)
            if stream_cfg.is_type_transformation:
                break

        if stream_cfg.value_type is None:
            raise ValueError(f"The value type of the InputStream with name '{name}' is not defined")

        ser = RuntimeHelpers[T](env).make_stream_serde(type_name=stream_cfg.value_type)
        if ser.value_serializer.is_stub:
            raise ValueError(f"Serializer for the type '{stream_cfg.value_type}' in the stream '{name}' can't be a stub serializer")

        super().__init__(name=name, env=env, serde=ser)

    async def consume_binary(self, data: BytesBuffer):
        if self._serde is None:
            raise ValueError(f"serde can not be None for InputSplitStream '{self.name}'")
        value = self._serde.deserialize(data)
        if self._caller is not None:
            await self._caller.consume(value)


class TypedBinaryKVSplitStream[K: Hashable, V](SplitStreamBase[KeyValue[K, V]], BinaryKVConsumer[K, V]):
    _kv_serde: TypedStreamKeyValueSerde[KeyValue[K, V]]

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"SplitStream configuration names '{name}' not found")

        kv_stream_cfg = cfg
        while kv_stream_cfg.id_source != 0:
            kv_stream_cfg = env.config.get_stream_config_by_id(kv_stream_cfg.id_source)
            if kv_stream_cfg.type == TransformationType.KeyBy:
                break

        if kv_stream_cfg.key_type is None:
            raise ValueError(f"The key type of the KeyByStream with name '{name}' is not defined")
        if kv_stream_cfg.value_type is None:
            raise ValueError(f"The value type of the KeyByStream with name '{name}' is not defined")

        kv_serde = RuntimeKeyValueHelpers[K, V](env).make_key_value_stream_serde(key_type_name=kv_stream_cfg.key_type,
                                    value_type_name=kv_stream_cfg.value_type)

        if kv_serde.key_serializer.is_stub:
            raise ValueError(f"Serializer for the key type '{cfg.key_type}' in the stream '{name}' can't be a stub serializer")
        if kv_serde.value_serializer.is_stub:
            raise ValueError(f"Serializer for the value type '{cfg.value_type}' in the stream '{name}' can't be a stub serializer")

        super().__init__(name=name,
                         env=env,
                         serde=kv_serde)
        self._kv_serde = kv_serde

    async def consume_binary(self, key_data: BytesBuffer, value_data: BytesBuffer):
        value = self._kv_serde.deserialize_key_value(key_data, value_data)
        if self._caller is not None:
            await self._caller.consume(value)


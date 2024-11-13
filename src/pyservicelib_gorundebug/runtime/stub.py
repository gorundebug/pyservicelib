#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Hashable, cast

from ..api.models.transformation_type import TransformationType
from .common import ServiceExecutionEnvironment, TypedBinaryConsumedStream
from .common import TypedBinaryKVConsumedStream, RuntimeHelpers
from .common import RuntimeKeyValueHelpers, TypedStreamConsumer, TypedStream, Consume
from .common import BinaryConsume, BinaryKVConsume
from .datastruct import KeyValue
from .serde import BytesBuffer, TypedStreamKeyValueSerde, TypedStreamSerde


class InStubStream[T](TypedBinaryConsumedStream[T]):

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"InputStream configuration named '{name}' not found")

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

        super().__init__(stream_id=cfg.id, env=env,
                         serde=ser)

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    async def consume_binary(self, data: BytesBuffer):
        if self._serde is None:
            raise ValueError(f"serde can not be None for InStubStream '{self.name}'")
        value = self._serde.deserialize(data)
        if self._caller is not None:
            await self._caller.consume(value)


class InStubKVStream[K:Hashable, V](TypedBinaryKVConsumedStream[K, V]):
    _kv_serde: TypedStreamKeyValueSerde[KeyValue[K, V]]

    def __init__(self, name: str, env: ServiceExecutionEnvironment):
        cfg = env.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"InputStream configuration named '{name}' not found")

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

        super().__init__(stream_id=cfg.id, env=env, serde=kv_serde)
        self._kv_serde = kv_serde

    async def consume(self, value: KeyValue[K, V]) -> None:
        if self._caller is not None:
            await self._caller.consume(value)

    async def consume_binary(self, key_data: BytesBuffer, value_data: BytesBuffer):
        value = self._kv_serde.deserialize_key_value(key_data, value_data)
        if self._caller is not None:
            await self._caller.consume(value)


class OutStubStream[T](TypedStreamConsumer[T]):
    _source: TypedStream[T]
    _consumer: Consume[T]

    def __init__(self, name: str, stream: TypedStream[T], consumer: Consume[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"OutStubStream configuration names '{name}' not found")

        super().__init__(stream_id=cfg.id, env=stream.environment)
        self._source = stream
        self._consumer = consumer
        stream.consumer = self

    async def consume(self, value: T) -> None:
        await self._consumer.consume(value)


class OutStubBinaryStream[T](TypedStreamConsumer[T]):
    _source: TypedStream[T]
    _consumer: BinaryConsume[T]
    _serde: TypedStreamSerde[T]

    def __init__(self, name: str, stream: TypedStream[T], consumer: BinaryConsume[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"OutStubStream configuration names '{name}' not found")
        if stream.serde.value_serializer.is_stub:
            raise ValueError(f"Serializer for the type '{stream.serde.value_serializer.type_name}' in the stream '{name}' can't be a stub serializer")

        super().__init__(stream_id=cfg.id, env=stream.environment)
        self._source = stream
        self._consumer = consumer
        self._serde = stream.serde
        stream.consumer = self

    async def consume(self, value: T) -> None:
        data = self._serde.serialize(value)
        await self._consumer.consume(data)


class OutStubBinaryKVStream[T](TypedStreamConsumer[T]):
    _source: TypedStream[T]
    _consumer: BinaryKVConsume[T]
    _kv_serde: TypedStreamKeyValueSerde[T]

    def __init__(self, name: str, stream: TypedStream[T], consumer: BinaryKVConsume[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"OutStubStream configuration names '{name}' not found")
        if not stream.serde.is_key_value:
            raise ValueError(f"Invalid serde type for OutStubStream '{name}'")

        kv_serde = cast(TypedStreamKeyValueSerde[T], stream.serde)
        if kv_serde.key_serializer.is_stub:
            raise ValueError(f"Serializer for the key type '{kv_serde.key_serializer.type_name}' in the stream '{name}' can't be a stub serializer")
        if kv_serde.value_serializer.is_stub:
            raise ValueError(f"Serializer for the value type '{kv_serde.value_serializer.type_name}' in the stream '{name}' can't be a stub serializer")

        super().__init__(stream_id=cfg.id, env=stream.environment)
        self._source = stream
        self._consumer = consumer
        self._kv_serde = kv_serde
        stream.consumer = self

    async def consume(self, value: T) -> None:
        key_data = self._kv_serde.serialize_key(value)
        value_data = self._kv_serde.serialize_key(value)
        await self._consumer.consume(key_data, value_data)

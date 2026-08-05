#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from typing import Any, Union, Self, cast, Optional, ClassVar
from pydantic import Field, ConfigDict, StrictStr
from dataclasses import dataclass
from abc import ABC, abstractmethod
import os
import re

def _to_camel_case(snake: str) -> str:
    parts = snake.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])

from ...api.models.data_type import DataType
from ...api.models.transformation_type import TransformationType
from ...api.models.stream import Stream
from ...api.models.data_connector import DataConnector
from ...api.models.endpoint import Endpoint
from ...api.models.pool import Pool
from ...api.models.type import Type
from ...api.models.service import Service
from ...api.models.link import Link
from ...api.models.module import Module
from ...api.models.call_semantics import CallSemantics
from ...api.models.project_settings import ProjectSettings as ApiProjectSettings
from ...api.models.stream_app import StreamApp


transformation_name_map = {
    TransformationType.CycleLink: "cycleLink",
    TransformationType.Sink: "sink",
    TransformationType.Filter: "filter",
    TransformationType.FlatMap: "flatMap",
    TransformationType.FlatMapIterable: "flatMapIterable",
    TransformationType.Process: "process",
    TransformationType.Input: "input",
    TransformationType.Join: "join",
    TransformationType.KeyBy: "keyBy",
    TransformationType.Map: "map",
    TransformationType.Merge: "merge",
    TransformationType.MultiJoin: "multiJoin",
    TransformationType.Split: "split",
    TransformationType.Delay: "delay",
    TransformationType.Error: "error",
    TransformationType.Case: "case",
    TransformationType.When: "when",
}

class ConfigSettings:
    pass

def _properties_getattr(obj: Any, item: str) -> Any:
    try:
        props = object.__getattribute__(obj, 'properties')
    except AttributeError:
        raise AttributeError(f'{type(obj).__name__!r} object has no attribute {item!r}')
    if props is not None:
        if item in props:
            return props[item]
        camel = _to_camel_case(item)
        if camel in props:
            return props[camel]
    raise AttributeError(f'{type(obj).__name__!r} object has no attribute {item!r}')


class StreamConfig(Stream):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

    @property
    def transformation_name(self) -> str:
        return transformation_name_map[self.type]

    @property
    def is_type_transformation(self) -> bool:
        return (self.type == TransformationType.Input or
                self.type == TransformationType.Map or
                self.type == TransformationType.Join or
                self.type == TransformationType.MultiJoin or
                self.type == TransformationType.FlatMap or
                self.type == TransformationType.FlatMapIterable or
                self.type == TransformationType.KeyBy or
                self.type == TransformationType.When)


class DataConnectorConfig(DataConnector):
    implementation: Optional[StrictStr] = Field(default=None)  # type: ignore[assignment]
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

class EndpointConfig(Endpoint):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        if item not in ('method', 'format'):
            return _properties_getattr(self, item)
        raise AttributeError(f'{type(self).__name__!r} object has no attribute {item!r}')

    @property
    def method(self) -> Optional[str]:
        if self.properties:
            m = self.properties.get('method')
            if m:
                return m
        if self.http_method_type:
            return str(self.http_method_type.value)
        return None

    @property
    def format(self) -> Optional[str]:
        if self.properties:
            return self.properties.get('format')
        return None

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class PoolConfig(Pool):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class ModuleConfig(Module):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        normalized = dict(obj)
        if "path" in normalized and "modulePath" not in normalized:
            normalized["modulePath"] = normalized.pop("path")
        normalized.setdefault("golangVersion", "")
        result = cls(**normalized)
        result.properties = obj
        return result


class TypeConfig(Type):
    primitive_types: ClassVar[set[str]] = {
        cast(str, DataType.int),
        cast(str, DataType.uint),
        cast(str, DataType.byte),
        cast(str, DataType.char),
        cast(str, DataType.boolean),
        cast(str, DataType.unicodeChar),
        cast(str, DataType.string),
        cast(str, DataType.unicodeString),
        cast(str, DataType.float),
        cast(str, DataType.double),
        cast(str, DataType.int8),
        cast(str, DataType.int16),
        cast(str, DataType.int32),
        cast(str, DataType.int64),
        cast(str, DataType.uint8),
        cast(str, DataType.uint16),
        cast(str, DataType.uint32),
        cast(str, DataType.uint64),
    }

    serde_type_map: ClassVar[dict[str, str]] = {
        cast(str, DataType.int): 'int',
        cast(str, DataType.uint): 'uint',
        cast(str, DataType.byte): 'int8',
        cast(str, DataType.char): 'rune',
        cast(str, DataType.boolean): 'bool',
        cast(str, DataType.unicodeChar): 'str',
        cast(str, DataType.string): 'str',
        cast(str, DataType.unicodeString): 'str',
        cast(str, DataType.float): 'float32',
        cast(str, DataType.double): 'float64',
        cast(str, DataType.int8): 'int8',
        cast(str, DataType.int16): 'int16',
        cast(str, DataType.int32): 'int32',
        cast(str, DataType.int64): 'int64',
        cast(str, DataType.uint8): 'uint8',
        cast(str, DataType.uint16): 'uint16',
        cast(str, DataType.uint32): 'uint32',
        cast(str, DataType.uint64): 'uint64',
    }

    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

    @classmethod
    def is_primitive_type(cls, value_type: str) -> bool:
        return value_type in TypeConfig.primitive_types

    @classmethod
    def get_serde_type(cls, value_type: str) -> str:
        return cls.serde_type_map[value_type]

    @property
    def serde_type(self) -> str:
        return TypeConfig.get_serde_type(cast(str, self.type))

    @property
    def is_primitive(self) -> bool:
        return TypeConfig.is_primitive_type(cast(str, self.type))

    @property
    def is_array(self) -> bool:
        return self.type == DataType.array

    @property
    def is_dict(self) -> bool:
        return self.type == DataType.map


class ServiceConfig(Service):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class LinkConfig(Link):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)

    def __init__(self, *, var_from: int, **data: Any) -> None:
        super().__init__(var_from=var_from, **data)

    def __getattr__(self, item: str) -> Any:
        return _properties_getattr(self, item)

    @property
    def income_call_semantics(self) -> Optional[CallSemantics]:
        try:
            return _properties_getattr(self, 'income_call_semantics')
        except AttributeError:
            return None

    @property
    def income_pool_name(self) -> Optional[str]:
        try:
            return _properties_getattr(self, 'income_pool_name')
        except AttributeError:
            return None

    @property
    def income_priority(self) -> Optional[int]:
        try:
            return _properties_getattr(self, 'income_priority')
        except AttributeError:
            return None

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        d = {("var_from" if k == "from" else k): v for k, v in obj.items()}
        _obj = cls(**d)
        _obj.properties = obj
        return _obj


class ProjectSettings(ApiProjectSettings):
    properties: Optional[dict[str, Any]] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

_ConfigType = Union[str, int, float, bool, list[Any], dict[str, Any]]

def _replace_placeholders(config: _ConfigType,
                          values: dict[str, Any]) -> Optional[_ConfigType]:
    if isinstance(config, str):
        if config.startswith("$"):
            placeholder = config[1:]
            return values.get(placeholder, config)
        return config
    elif isinstance(config, dict):
        return {key: _replace_placeholders(value, values) for key, value in config.items()}
    elif isinstance(config, list):
        return [_replace_placeholders(item, values) for item in config]
    else:
        return config

def replace_placeholders(config: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    result = _replace_placeholders(config, values)
    if not isinstance(result, dict):
        raise ValueError("The result must be a dictionary.")
    return result


_ENV_VAR_RE = re.compile(r'\$\{([^}]+)\}')


def _apply_env_to_value(v: _ConfigType) -> Optional[_ConfigType]:
    if isinstance(v, str):
        return _ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), m.group(0)), v)
    if isinstance(v, dict):
        return {k: _apply_env_to_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_apply_env_to_value(item) for item in v]
    return v


def apply_environment(config: dict[str, Any]) -> dict[str, Any]:
    """Replace ``${VAR_NAME}`` placeholders with values from os.environ.

    Unknown variable references are left unchanged.
    """
    result = _apply_env_to_value(config)
    if not isinstance(result, dict):
        raise ValueError("The result must be a dictionary.")
    return result

@dataclass(frozen=True, slots=True)
class LinkId:
    from_id: int
    to_id: int

class RuntimeConfig:
    streams_by_name: dict[str, StreamConfig]
    services_by_name: dict[str, ServiceConfig]
    links_by_id: dict[LinkId, LinkConfig]
    data_connectors_by_name: dict[str, DataConnectorConfig]
    endpoints_by_name: dict[str, EndpointConfig]
    streams_by_id: dict[int, StreamConfig]
    services_by_id: dict[int, ServiceConfig]
    data_connectors_by_id: dict[int, DataConnectorConfig]
    endpoints_by_id: dict[int, EndpointConfig]
    pool_by_name: dict[str, PoolConfig]
    types: dict[str, TypeConfig]
    modules: dict[str, ModuleConfig]

    def __init__(self):
        self.streams_by_name = {}
        self.services_by_name = {}
        self.links_by_id = {}
        self.data_connectors_by_name = {}
        self.endpoints_by_name = {}
        self.streams_by_id = {}
        self.services_by_id = {}
        self.data_connectors_by_id = {}
        self.endpoints_by_id = {}
        self.pool_by_name = {}
        self.types = {}
        self.modules = {}


class Config(ABC):

    @property
    @abstractmethod
    def config(self) -> "ServiceAppConfig":
        pass

class ServiceAppConfig(StreamApp, Config):
    runtime_config: RuntimeConfig = Field(default=None, exclude=True)  # type: ignore[assignment]
    log_level: StrictStr

    def __init__(self, **data):
        super().__init__(**data)
        self.runtime_config = RuntimeConfig()
        self.init_runtime_config()

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def _load_config(cls, obj: dict[str, Any]) -> dict[str, Any]:
        data = cls.load_config(obj)
        data = data | {
            "services": [ServiceConfig.from_dict(service_data)
                         for service_data in obj.get('services', [])],
            "streams": [StreamConfig.from_dict(stream_data)
                        for stream_data in obj.get('streams', [])],
            "links": [LinkConfig.from_dict(link_data)
                      for link_data in obj.get('links', [])],
            "types": [TypeConfig.from_dict(type_data)
                      for type_data in obj.get('types', [])],
            "modules": [ModuleConfig.from_dict(module_data)
                        for module_data in obj.get('modules', [])],
            "pools": [PoolConfig.from_dict(pool_data)
                      for pool_data in obj.get('pools', [])],
            "data_connectors": [DataConnectorConfig.from_dict(data_connector_data)
                                for data_connector_data in obj.get('dataConnectors', [])],
            "endpoints": [EndpointConfig.from_dict(endpoint_data)
                          for endpoint_data in obj.get('endpoints', [])],
            "settings": ProjectSettings.from_dict(obj.get('settings', {})),
            "log_level": 'DEBUG'
        }
        return data

    @classmethod
    def load_config(cls, obj: dict[str, Any]) -> dict[str, Any]:
        return {}

    @classmethod
    def from_dict(cls, obj: Optional[dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        cfg = cls.model_validate(cls._load_config(obj))
        cfg.init_runtime_config()
        return cfg

    def init_runtime_config(self):
        self.runtime_config = RuntimeConfig()

        for stream in self.streams:
            if stream.name in self.runtime_config.streams_by_name:
                raise ValueError(f"duplicate stream name: {stream.name}")
            if stream.id in self.runtime_config.streams_by_id:
                raise ValueError(f"duplicate stream id: {stream.id}")
            self.runtime_config.streams_by_name[stream.name] = cast(StreamConfig, stream)
            self.runtime_config.streams_by_id[stream.id] = cast(StreamConfig, stream)

        for service in self.services:
            if service.name in self.runtime_config.services_by_name:
                raise ValueError(f"duplicate service name: {service.name}")
            if service.id in self.runtime_config.services_by_id:
                raise ValueError(f"duplicate service id: {service.id}")
            self.runtime_config.services_by_name[service.name] = cast(ServiceConfig, service)
            self.runtime_config.services_by_id[service.id] = cast(ServiceConfig, service)

        for endpoint in self.endpoints:
            if endpoint.name in self.runtime_config.endpoints_by_name:
                raise ValueError(f"duplicate endpoint name: {endpoint.name}")
            if endpoint.id in self.runtime_config.endpoints_by_id:
                raise ValueError(f"duplicate endpoint id: {endpoint.id}")
            self.runtime_config.endpoints_by_name[endpoint.name] = cast(EndpointConfig, endpoint)
            self.runtime_config.endpoints_by_id[endpoint.id] = cast(EndpointConfig, endpoint)

        for data_connector in self.data_connectors:
            if data_connector.name in self.runtime_config.data_connectors_by_name:
                raise ValueError(f"duplicate data connector name: {data_connector.name}")
            if data_connector.id in self.runtime_config.data_connectors_by_id:
                raise ValueError(f"duplicate data connector id: {data_connector.id}")
            self.runtime_config.data_connectors_by_name[data_connector.name] = (
                cast(DataConnectorConfig, data_connector))
            self.runtime_config.data_connectors_by_id[data_connector.id] = (
                cast(DataConnectorConfig, data_connector))

        for module in self.modules or []:
            if module.name in self.runtime_config.modules:
                raise ValueError(f"duplicate module name: {module.name}")
            module_config = (
                module
                if isinstance(module, ModuleConfig)
                else ModuleConfig.from_dict(module.to_dict())
            )
            if module_config is None:
                raise ValueError(f"invalid module config: {module.name}")
            self.runtime_config.modules[module.name] = module_config

        for pool in self.pools:
            if pool.name in self.runtime_config.pool_by_name:
                raise ValueError(f"duplicate pool name: {pool.name}")
            self.runtime_config.pool_by_name[pool.name] = cast(PoolConfig, pool)

        for tp in self.types:
            if tp.name in self.runtime_config.types:
                raise ValueError(f"duplicate type name: {tp.name}")
            self.runtime_config.types[tp.name] = cast(TypeConfig, tp)

        for link in self.links:
            link_id = LinkId(from_id=link.var_from, to_id=link.to)
            if link_id in self.runtime_config.links_by_id:
                raise ValueError(f"duplicate link from={link.var_from} to={link.to}")
            self.runtime_config.links_by_id[link_id] = cast(LinkConfig, link)

    def get_stream_config_by_name(self, name: str) -> Optional[StreamConfig]:
        return self.runtime_config.streams_by_name.get(name)

    def get_data_connector_by_id(self, data_connector_id: int) -> DataConnectorConfig:
        return self.runtime_config.data_connectors_by_id[data_connector_id]

    def get_endpoint_config_by_id(self, endpoint_id: int) -> EndpointConfig:
        return self.runtime_config.endpoints_by_id[endpoint_id]

    def get_service_config_by_name(self, name: str) -> Optional[ServiceConfig]:
        return self.runtime_config.services_by_name.get(name)

    def get_service_config_by_id(self, service_id: int) -> ServiceConfig:
        return self.runtime_config.services_by_id[service_id]

    def get_stream_config_by_id(self,stream_id: int) -> StreamConfig:
        return self.runtime_config.streams_by_id[stream_id]

    def get_pool_by_name(self, name: str) -> Optional[PoolConfig]:
        return self.runtime_config.pool_by_name.get(name)

    def get_type_by_name(self, name: str) -> Optional[TypeConfig]:
        return self.runtime_config.types.get(name)

    def get_link(self, from_id: int, to_id: int) -> Optional[LinkConfig]:
        link_id = LinkId(from_id=from_id, to_id=to_id)
        return self.runtime_config.links_by_id.get(link_id)

    def get_input_stream_config(self, name: str):
        from .stream_types import InputStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return InputStreamConfig(cfg) if cfg is not None else None

    def get_map_stream_config(self, name: str):
        from .stream_types import MapStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return MapStreamConfig(cfg) if cfg is not None else None

    def get_filter_stream_config(self, name: str):
        from .stream_types import FilterStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return FilterStreamConfig(cfg) if cfg is not None else None

    def get_flatmap_stream_config(self, name: str):
        from .stream_types import FlatMapStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return FlatMapStreamConfig(cfg) if cfg is not None else None

    def get_flatmap_iterable_stream_config(self, name: str):
        from .stream_types import FlatMapIterableStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return FlatMapIterableStreamConfig(cfg) if cfg is not None else None

    def get_join_stream_config(self, name: str):
        from .stream_types import JoinStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return JoinStreamConfig(cfg) if cfg is not None else None

    def get_multi_join_stream_config(self, name: str):
        from .stream_types import MultiJoinStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return MultiJoinStreamConfig(cfg) if cfg is not None else None

    def get_process_stream_config(self, name: str):
        from .stream_types import ProcessStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return ProcessStreamConfig(cfg) if cfg is not None else None

    def get_key_by_stream_config(self, name: str):
        from .stream_types import KeyByStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return KeyByStreamConfig(cfg) if cfg is not None else None

    def get_merge_stream_config(self, name: str):
        from .stream_types import MergeStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return MergeStreamConfig(cfg) if cfg is not None else None

    def get_split_stream_config(self, name: str):
        from .stream_types import SplitStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return SplitStreamConfig(cfg) if cfg is not None else None

    def get_delay_stream_config(self, name: str):
        from .stream_types import DelayStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return DelayStreamConfig(cfg) if cfg is not None else None

    def get_sink_stream_config(self, name: str):
        from .stream_types import SinkStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return SinkStreamConfig(cfg) if cfg is not None else None

    def get_cycle_link_stream_config(self, name: str):
        from .stream_types import CycleLinkStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return CycleLinkStreamConfig(cfg) if cfg is not None else None

    def get_case_stream_config(self, name: str):
        from .stream_types import CaseStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return CaseStreamConfig(cfg) if cfg is not None else None

    def get_when_stream_config(self, name: str):
        from .stream_types import WhenStreamConfig
        cfg = self.get_stream_config_by_name(name)
        return WhenStreamConfig(cfg) if cfg is not None else None

    @property
    def config(self) -> "ServiceAppConfig":
        return self

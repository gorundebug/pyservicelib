#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from typing import Any, Dict, List, Union, Self, cast, Optional, ClassVar
from pydantic import Field, ConfigDict, StrictStr
from dataclasses import dataclass
from abc import ABC, abstractmethod

from pyservicelib.api.models.data_type import DataType
from pyservicelib.api.models.transformation_type import TransformationType
from pyservicelib.api.models.stream import Stream
from pyservicelib.api.models.data_connector import DataConnector
from pyservicelib.api.models.endpoint import Endpoint
from pyservicelib.api.models.pool import Pool
from pyservicelib.api.models.type import Type
from pyservicelib.api.models.service import Service
from pyservicelib.api.models.link import Link
from pyservicelib.api.models.project_settings import ProjectSettings as ApiProjectSettings
from pyservicelib.api.models.stream_app import StreamApp


transformation_name_map = {
    TransformationType.AppSink: "appSink",
    TransformationType.CycleLink: "cycleLink",
    TransformationType.Sink: "sink",
    TransformationType.Filter: "filter",
    TransformationType.FlatMap: "flatMap",
    TransformationType.FlatMapIterable: "flatMapIterable",
    TransformationType.ForEach: "forEach",
    TransformationType.Input: "input",
    TransformationType.Join: "join",
    TransformationType.KeyBy: "keyBy",
    TransformationType.Map: "map",
    TransformationType.Merge: "merge",
    TransformationType.MultiJoin: "multiJoin",
    TransformationType.Parallels: "parallels",
    TransformationType.Split: "split",
}

class ConfigSettings:
    pass

class StreamConfig(Stream):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

    @property
    def transformation_name(self) -> str:
        return transformation_name_map[self.type]

class DataConnectorConfig(DataConnector):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

class EndpointConfig(Endpoint):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class PoolConfig(Pool):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class TypeConfig(Type):
    primitive_types: ClassVar[set[str]] = {
        cast(str, DataType.INT),
        cast(str, DataType.UINT),
        cast(str, DataType.BYTE),
        cast(str, DataType.CHAR),
        cast(str, DataType.BOOLEAN),
        cast(str, DataType.UNICODE_CHAR),
        cast(str, DataType.STRING),
        cast(str, DataType.UNICODE_STRING),
        cast(str, DataType.FLOAT),
        cast(str, DataType.DOUBLE),
        cast(str, DataType.INT8),
        cast(str, DataType.INT16),
        cast(str, DataType.INT32),
        cast(str, DataType.INT64),
        cast(str, DataType.UINT8),
        cast(str, DataType.UINT16),
        cast(str, DataType.UINT32),
        cast(str, DataType.UINT64)
    }

    serde_type_map: ClassVar[dict[str, str]] = {
        cast(str, DataType.INT): 'int',
        cast(str, DataType.UINT): 'uint',
        cast(str, DataType.BYTE): 'int8',
        cast(str, DataType.CHAR): 'int32',
        cast(str, DataType.BOOLEAN): 'bool',
        cast(str, DataType.UNICODE_CHAR): 'str',
        cast(str, DataType.STRING): 'str',
        cast(str, DataType.UNICODE_STRING): 'str',
        cast(str, DataType.FLOAT): 'float32,',
        cast(str, DataType.DOUBLE): 'float64',
        cast(str, DataType.INT8): 'int8',
        cast(str, DataType.INT16): 'int16',
        cast(str, DataType.INT32): 'int32',
        cast(str, DataType.INT64): 'int64',
        cast(str, DataType.UINT8): 'uint8',
        cast(str, DataType.UINT16): 'uint16',
        cast(str, DataType.UINT32): 'uint32',
        cast(str, DataType.UINT64): 'uint64'
    }

    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
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
        return self.type == DataType.ARRAY

    @property
    def is_dict(self) -> bool:
        return self.type == DataType.MAP


class ServiceConfig(Service):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class LinkConfig(Link):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


class ProjectSettings(ApiProjectSettings):
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj

_ConfigType = Union[str, int, float, bool, List[Any], Dict[str, Any]]

def _replace_placeholders(config: _ConfigType,
                          values: Dict[str, Any]) -> Optional[_ConfigType]:
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

def replace_placeholders(config: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    result = _replace_placeholders(config, values)
    if not isinstance(result, dict):
        raise ValueError("The result must be a dictionary.")
    return result

@dataclass(frozen=True)
class LinkId:
    from_id: int
    to_id: int

class RuntimeConfig:
    streams_by_name: Dict[str, StreamConfig]
    services_by_name: Dict[str, ServiceConfig]
    links_by_id: Dict[LinkId, LinkConfig]
    data_connectors_by_name: Dict[str, DataConnectorConfig]
    endpoints_by_name: Dict[str, EndpointConfig]
    streams_by_id: Dict[int, StreamConfig]
    services_by_id: Dict[int, ServiceConfig]
    data_connectors_by_id: Dict[int, DataConnectorConfig]
    endpoints_by_id: Dict[int, EndpointConfig]
    pool_by_name: Dict[str, PoolConfig]
    types: Dict[str, TypeConfig]

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


class Config(ABC):

    @property
    @abstractmethod
    def config(self) -> "ServiceAppConfig":
        pass

class ServiceAppConfig(StreamApp, Config):
    runtime_config: RuntimeConfig = Field(default=None, exclude=True)
    log_level: StrictStr

    def __init__(self, **data):
        super().__init__(**data)
        self.runtime_config = RuntimeConfig()
        self.init_runtime_config()

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def _load_config(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
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
    def load_config(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        cfg = cls.model_validate(cls._load_config(obj))
        cfg.init_runtime_config()
        return cfg

    def init_runtime_config(self):
        for stream in self.streams:
            self.runtime_config.streams_by_name[stream.name] = cast(StreamConfig, stream)
            self.runtime_config.streams_by_id[stream.id] = cast(StreamConfig, stream)

        for service in self.services:
            self.runtime_config.services_by_name[service.name] = cast(ServiceConfig, service)
            self.runtime_config.services_by_id[service.id] = cast(ServiceConfig, service)

        for endpoint in self.endpoints:
            self.runtime_config.endpoints_by_name[endpoint.name] = cast(EndpointConfig, endpoint)
            self.runtime_config.endpoints_by_id[endpoint.id] = cast(EndpointConfig, endpoint)

        for data_connector in self.data_connectors:
            self.runtime_config.data_connectors_by_name[data_connector.name] = (
                cast(DataConnectorConfig, data_connector))
            self.runtime_config.data_connectors_by_id[data_connector.id] = (
                cast(DataConnectorConfig, data_connector))

        for pool in self.pools:
            self.runtime_config.pool_by_name[pool.name] = cast(PoolConfig, pool)

        for tp in self.types:
            self.runtime_config.types[tp.name] = cast(TypeConfig, tp)

        for link in self.links:
            link_id = LinkId(from_id=link.var_from, to_id=link.to)
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

    @property
    def config(self) -> "ServiceAppConfig":
        return self
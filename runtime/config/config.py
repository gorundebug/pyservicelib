#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

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
from typing import Any, Dict, List, Union, Self, cast, Optional
from pydantic import Field, ConfigDict
from dataclasses import dataclass
from abc import ABC, abstractmethod


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
    properties: Dict[str, Any] = Field(default=None, exclude=True)
    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        _obj = cls(**obj)
        _obj.properties = obj
        return _obj


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


class Config(ABC):

    @property
    @abstractmethod
    def service_config(self) -> "ServiceAppConfig":
        pass

class ServiceAppConfig(StreamApp, Config):
    runtime_config: RuntimeConfig = Field(default=None, exclude=True)

    def __init__(self, **data):
        super().__init__(**data)
        self.runtime_config = RuntimeConfig()
        self.init_runtime_config()

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        if obj is None:
            return None
        return cls(
            services=[ServiceConfig.from_dict(service_data)
                      for service_data in obj.get('services', [])],
            streams=[StreamConfig.from_dict(stream_data)
                     for stream_data in obj.get('streams', [])],
            links=[LinkConfig.from_dict(link_data)
                   for link_data in obj.get('links', [])],
            types=[TypeConfig.from_dict(type_data)
                   for type_data in obj.get('types', [])],
            pools=[PoolConfig.from_dict(pool_data)
                   for pool_data in obj.get('pools', [])],
            data_connectors=[DataConnectorConfig.from_dict(data_connector_data)
                             for data_connector_data in obj.get('dataConnectors', [])],
            endpoints=[EndpointConfig.from_dict(endpoint_data)
                       for endpoint_data in obj.get('endpoints', [])],
            settings=ProjectSettings.from_dict(obj.get('settings', {}))
        )

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

        for link in self.links:
            link_id = LinkId(from_id=link.var_from, to_id=link.to)
            self.runtime_config.links_by_id[link_id] = cast(LinkConfig, link)

    def get_stream_config_by_name(self, name: str) -> Optional[StreamConfig]:
        return self.runtime_config.streams_by_name.get(name)

    def get_data_connector_by_id(self, data_connector_id: int) -> Optional[DataConnectorConfig]:
        return self.runtime_config.data_connectors_by_id.get(data_connector_id)

    def get_endpoint_config_by_id(self, endpoint_id: int) -> Optional[EndpointConfig]:
        return self.runtime_config.endpoints_by_id.get(endpoint_id)

    def get_service_config_by_name(self, name: str) -> Optional[ServiceConfig]:
        return self.runtime_config.services_by_name.get(name)

    def get_service_config_by_id(self, service_id: int) -> Optional[ServiceConfig]:
        return self.runtime_config.services_by_id.get(service_id)

    def get_stream_config_by_id(self,stream_id: int) -> Optional[StreamConfig]:
        return self.runtime_config.streams_by_id.get(stream_id)

    def get_pool_by_name(self, name: str) -> Optional[PoolConfig]:
        return self.runtime_config.pool_by_name.get(name)

    def get_link(self, from_id: int, to_id: int) -> Optional[LinkConfig]:
        link_id = LinkId(from_id=from_id, to_id=to_id)
        return self.runtime_config.links_by_id.get(link_id)

    @property
    def service_config(self) -> "ServiceAppConfig":
        return self



class ServiceEnvironmentConfig(ABC):

    @property
    @abstractmethod
    def config(self) -> ServiceAppConfig:
        pass

    @property
    @abstractmethod
    def service_config(self) -> ServiceConfig:
        pass
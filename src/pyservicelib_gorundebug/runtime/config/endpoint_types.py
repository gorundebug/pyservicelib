#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from typing import Optional, Any
from ...api.models.http_method_type import HTTPMethodType
from ...api.models.grpc_method_type import GrpcMethodType


class EndpointConfig(ABC):

    @property
    @abstractmethod
    def id(self) -> int:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def id_data_connector(self) -> int:
        pass

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        pass

    def get_property(self, name: str) -> Any:
        return None


class HttpEndpointConfig(EndpointConfig):

    def __init__(self, id: int, name: str, id_data_connector: int,
                 http_method_type: Optional[HTTPMethodType] = None,
                 path: Optional[str] = None,
                 function_name: Optional[str] = None,
                 function_package: Optional[str] = None,
                 public_function: bool = False,
                 function_description: Optional[str] = None,
                 function_initializer_group: Optional[str] = None,
                 function_module: Optional[str] = None,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._http_method_type = http_method_type
        self._path = path
        self._function_name = function_name
        self._function_package = function_package
        self._public_function = public_function
        self._function_description = function_description
        self._function_initializer_group = function_initializer_group
        self._function_module = function_module
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def id_data_connector(self) -> int:
        return self._id_data_connector

    @property
    def http_method_type(self) -> Optional[HTTPMethodType]:
        return self._http_method_type

    @property
    def path(self) -> Optional[str]:
        return self._path

    @property
    def function_name(self) -> Optional[str]:
        return self._function_name

    @property
    def function_package(self) -> Optional[str]:
        return self._function_package

    @property
    def public_function(self) -> bool:
        return self._public_function

    @property
    def function_description(self) -> Optional[str]:
        return self._function_description

    @property
    def function_initializer_group(self) -> Optional[str]:
        return self._function_initializer_group

    @property
    def function_module(self) -> Optional[str]:
        return self._function_module

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "idDataConnector": self._id_data_connector,
        }
        if self._http_method_type is not None:
            result["httpMethodType"] = self._http_method_type.value
        if self._path is not None:
            result["path"] = self._path
        if self._function_name is not None:
            result["functionName"] = self._function_name
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class GrpcEndpointConfig(EndpointConfig):

    def __init__(self, id: int, name: str, id_data_connector: int,
                 grpc_method_type: Optional[GrpcMethodType] = None,
                 method_name: Optional[str] = None,
                 function_name: Optional[str] = None,
                 function_package: Optional[str] = None,
                 public_function: bool = False,
                 function_description: Optional[str] = None,
                 function_initializer_group: Optional[str] = None,
                 function_module: Optional[str] = None,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._grpc_method_type = grpc_method_type
        self._method_name = method_name
        self._function_name = function_name
        self._function_package = function_package
        self._public_function = public_function
        self._function_description = function_description
        self._function_initializer_group = function_initializer_group
        self._function_module = function_module
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def id_data_connector(self) -> int:
        return self._id_data_connector

    @property
    def grpc_method_type(self) -> Optional[GrpcMethodType]:
        return self._grpc_method_type

    @property
    def method_name(self) -> Optional[str]:
        return self._method_name

    @property
    def function_name(self) -> Optional[str]:
        return self._function_name

    @property
    def function_package(self) -> Optional[str]:
        return self._function_package

    @property
    def public_function(self) -> bool:
        return self._public_function

    @property
    def function_description(self) -> Optional[str]:
        return self._function_description

    @property
    def function_initializer_group(self) -> Optional[str]:
        return self._function_initializer_group

    @property
    def function_module(self) -> Optional[str]:
        return self._function_module

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "idDataConnector": self._id_data_connector,
        }
        if self._grpc_method_type is not None:
            result["grpcMethodType"] = self._grpc_method_type.value
        if self._method_name is not None:
            result["methodName"] = self._method_name
        if self._function_name is not None:
            result["functionName"] = self._function_name
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class KafkaEndpointConfig(EndpointConfig):

    def __init__(self, id: int, name: str, id_data_connector: int,
                 topic: Optional[str] = None,
                 consumer_group: Optional[str] = None,
                 create_topic: bool = False,
                 partitions: int = 0,
                 replication_factor: int = 0,
                 function_name: Optional[str] = None,
                 function_package: Optional[str] = None,
                 public_function: bool = False,
                 function_description: Optional[str] = None,
                 function_initializer_group: Optional[str] = None,
                 function_module: Optional[str] = None,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._topic = topic
        self._consumer_group = consumer_group
        self._create_topic = create_topic
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._function_name = function_name
        self._function_package = function_package
        self._public_function = public_function
        self._function_description = function_description
        self._function_initializer_group = function_initializer_group
        self._function_module = function_module
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def id_data_connector(self) -> int:
        return self._id_data_connector

    @property
    def topic(self) -> Optional[str]:
        return self._topic

    @property
    def consumer_group(self) -> Optional[str]:
        return self._consumer_group

    @property
    def create_topic(self) -> bool:
        return self._create_topic

    @property
    def partitions(self) -> int:
        return self._partitions

    @property
    def replication_factor(self) -> int:
        return self._replication_factor

    @property
    def function_name(self) -> Optional[str]:
        return self._function_name

    @property
    def function_package(self) -> Optional[str]:
        return self._function_package

    @property
    def public_function(self) -> bool:
        return self._public_function

    @property
    def function_description(self) -> Optional[str]:
        return self._function_description

    @property
    def function_initializer_group(self) -> Optional[str]:
        return self._function_initializer_group

    @property
    def function_module(self) -> Optional[str]:
        return self._function_module

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "idDataConnector": self._id_data_connector,
        }
        if self._topic is not None:
            result["topic"] = self._topic
        if self._consumer_group is not None:
            result["consumerGroup"] = self._consumer_group
        if self._create_topic:
            result["createTopic"] = self._create_topic
        if self._partitions:
            result["partitions"] = self._partitions
        if self._replication_factor:
            result["replicationFactor"] = self._replication_factor
        if self._function_name is not None:
            result["functionName"] = self._function_name
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class CustomEndpointConfig(EndpointConfig):

    def __init__(self, id: int, name: str, id_data_connector: int,
                 function_name: Optional[str] = None,
                 function_package: Optional[str] = None,
                 public_function: bool = False,
                 function_description: Optional[str] = None,
                 function_initializer_group: Optional[str] = None,
                 function_module: Optional[str] = None,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._function_name = function_name
        self._function_package = function_package
        self._public_function = public_function
        self._function_description = function_description
        self._function_initializer_group = function_initializer_group
        self._function_module = function_module
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def id_data_connector(self) -> int:
        return self._id_data_connector

    @property
    def function_name(self) -> Optional[str]:
        return self._function_name

    @property
    def function_package(self) -> Optional[str]:
        return self._function_package

    @property
    def public_function(self) -> bool:
        return self._public_function

    @property
    def function_description(self) -> Optional[str]:
        return self._function_description

    @property
    def function_initializer_group(self) -> Optional[str]:
        return self._function_initializer_group

    @property
    def function_module(self) -> Optional[str]:
        return self._function_module

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "idDataConnector": self._id_data_connector,
        }
        if self._function_name is not None:
            result["functionName"] = self._function_name
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)

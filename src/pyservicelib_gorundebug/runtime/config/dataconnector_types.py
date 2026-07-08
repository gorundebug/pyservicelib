#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from typing import Optional, Any
from ...api.models.data_connector_type import DataConnectorType
from ...api.models.data_connector_implementation import DataConnectorImplementation


class DataConnectorConfig(ABC):

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
    def type(self) -> DataConnectorType:
        pass

    @property
    @abstractmethod
    def implementation(self) -> DataConnectorImplementation:
        pass

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        pass

    def get_property(self, name: str) -> Any:
        return None


class HttpDataConnectorConfig(DataConnectorConfig):

    def __init__(self, id: int, name: str, implementation: DataConnectorImplementation,
                 host: Optional[str] = None, port: Optional[int] = None,
                 module: Optional[str] = None, use_dedicated_listener: bool = False,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._implementation = implementation
        self._host = host
        self._port = port
        self._module = module
        self._use_dedicated_listener = use_dedicated_listener
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> DataConnectorType:
        return DataConnectorType.HTTP

    @property
    def implementation(self) -> DataConnectorImplementation:
        return self._implementation

    @property
    def host(self) -> Optional[str]:
        return self._host

    @property
    def port(self) -> Optional[int]:
        return self._port

    @property
    def module(self) -> Optional[str]:
        return self._module

    @property
    def use_dedicated_listener(self) -> bool:
        return self._use_dedicated_listener

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "type": self.type.value,
            "implementation": self._implementation.value,
        }
        if self._host is not None:
            result["host"] = self._host
        if self._port is not None:
            result["port"] = self._port
        if self._module is not None:
            result["module"] = self._module
        if self._use_dedicated_listener:
            result["useDedicatedListener"] = self._use_dedicated_listener
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class GrpcDataConnectorConfig(DataConnectorConfig):

    def __init__(self, id: int, name: str, implementation: DataConnectorImplementation,
                 address: Optional[str] = None, module: Optional[str] = None,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._implementation = implementation
        self._address = address
        self._module = module
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> DataConnectorType:
        return DataConnectorType.gRPC

    @property
    def implementation(self) -> DataConnectorImplementation:
        return self._implementation

    @property
    def address(self) -> Optional[str]:
        return self._address

    @property
    def module(self) -> Optional[str]:
        return self._module

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "type": self.type.value,
            "implementation": self._implementation.value,
        }
        if self._address is not None:
            result["address"] = self._address
        if self._module is not None:
            result["module"] = self._module
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class KafkaDataConnectorConfig(DataConnectorConfig):

    def __init__(self, id: int, name: str, implementation: DataConnectorImplementation,
                 brokers: Optional[str] = None, version: Optional[str] = None,
                 dial_timeout: float = 0.0, use_partitioner: bool = False,
                 async_: bool = False, properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._implementation = implementation
        self._brokers = brokers
        self._version = version
        self._dial_timeout = dial_timeout
        self._use_partitioner = use_partitioner
        self._async = async_
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> DataConnectorType:
        return DataConnectorType.Kafka

    @property
    def implementation(self) -> DataConnectorImplementation:
        return self._implementation

    @property
    def brokers(self) -> Optional[str]:
        return self._brokers

    @property
    def version(self) -> Optional[str]:
        return self._version

    @property
    def dial_timeout(self) -> float:
        return self._dial_timeout

    @property
    def use_partitioner(self) -> bool:
        return self._use_partitioner

    @property
    def async_(self) -> bool:
        return self._async

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self._id,
            "name": self._name,
            "type": self.type.value,
            "implementation": self._implementation.value,
        }
        if self._brokers is not None:
            result["brokers"] = self._brokers
        if self._version is not None:
            result["version"] = self._version
        if self._dial_timeout:
            result["dialTimeout"] = self._dial_timeout
        if self._use_partitioner:
            result["usePartitioner"] = self._use_partitioner
        if self._async:
            result["async"] = self._async
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class CustomDataConnectorConfig(DataConnectorConfig):

    def __init__(self, id: int, name: str, implementation: DataConnectorImplementation,
                 properties: Optional[dict[str, Any]] = None):
        self._id = id
        self._name = name
        self._implementation = implementation
        self._properties = properties or {}

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> DataConnectorType:
        return DataConnectorType.Custom

    @property
    def implementation(self) -> DataConnectorImplementation:
        return self._implementation

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "name": self._name,
            "type": self.type.value,
            "implementation": self._implementation.value,
        }

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)

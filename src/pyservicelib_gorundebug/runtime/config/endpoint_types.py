#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from typing import Optional, Any
from ...api.models.http_method_type import HTTPMethodType
from ...api.models.grpc_method_type import GrpcMethodType
from ...api.models.schedule_missed_run_policy import ScheduleMissedRunPolicy
from ...api.models.schedule_overlap_policy import ScheduleOverlapPolicy
from ...api.models.temporal_execution_type import TemporalExecutionType


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

    @property
    def tracing_enabled(self) -> bool:
        return getattr(self, "_tracing_enabled", False)


class HttpEndpointConfig(EndpointConfig):
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        tracing_enabled: bool = False,
        http_method_type: Optional[HTTPMethodType] = None,
        path: Optional[str] = None,
        function_name: Optional[str] = None,
        function_package: Optional[str] = None,
        public_function: bool = False,
        function_description: Optional[str] = None,
        function_initializer_group: Optional[str] = None,
        function_module: Optional[str] = None,
        properties: Optional[dict[str, Any]] = None,
    ):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._tracing_enabled = tracing_enabled
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
            "tracingEnabled": self._tracing_enabled,
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
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        tracing_enabled: bool = False,
        grpc_method_type: Optional[GrpcMethodType] = None,
        method_name: Optional[str] = None,
        function_name: Optional[str] = None,
        function_package: Optional[str] = None,
        public_function: bool = False,
        function_description: Optional[str] = None,
        function_initializer_group: Optional[str] = None,
        function_module: Optional[str] = None,
        properties: Optional[dict[str, Any]] = None,
    ):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._tracing_enabled = tracing_enabled
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
            "tracingEnabled": self._tracing_enabled,
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
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        tracing_enabled: bool = False,
        topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        enabled: bool = False,
        create_topic: bool = False,
        partitions: int = 0,
        replication_factor: int = 0,
        function_name: Optional[str] = None,
        function_package: Optional[str] = None,
        public_function: bool = False,
        function_description: Optional[str] = None,
        function_initializer_group: Optional[str] = None,
        function_module: Optional[str] = None,
        properties: Optional[dict[str, Any]] = None,
    ):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._tracing_enabled = tracing_enabled
        self._topic = topic
        self._consumer_group = consumer_group
        self._enabled = enabled
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
    def enabled(self) -> bool:
        return self._enabled

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
            "enabled": self._enabled,
            "tracingEnabled": self._tracing_enabled,
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
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        tracing_enabled: bool = False,
        function_name: Optional[str] = None,
        function_package: Optional[str] = None,
        public_function: bool = False,
        function_description: Optional[str] = None,
        function_initializer_group: Optional[str] = None,
        function_module: Optional[str] = None,
        properties: Optional[dict[str, Any]] = None,
    ):
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._tracing_enabled = tracing_enabled
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
            "tracingEnabled": self._tracing_enabled,
        }
        if self._function_name is not None:
            result["functionName"] = self._function_name
        return result

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class CronEndpointConfig(EndpointConfig):
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        enabled: bool = False,
        tracing_enabled: bool = False,
        schedule: str = "",
        timezone: str = "UTC",
        overlap_policy: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP,
        missed_run_policy: ScheduleMissedRunPolicy = ScheduleMissedRunPolicy.SKIP,
        properties: Optional[dict[str, Any]] = None,
    ):
        if timezone != "UTC":
            raise ValueError("scheduled endpoint timezone must be UTC")
        self._id = id
        self._name = name
        self._id_data_connector = id_data_connector
        self._enabled = enabled
        self._tracing_enabled = tracing_enabled
        self._schedule = schedule
        self._timezone = timezone
        self._overlap_policy = overlap_policy
        self._missed_run_policy = missed_run_policy
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
    def enabled(self) -> bool:
        return self._enabled

    @property
    def schedule(self) -> str:
        return self._schedule

    @property
    def timezone(self) -> str:
        return self._timezone

    @property
    def overlap_policy(self) -> ScheduleOverlapPolicy:
        return self._overlap_policy

    @property
    def missed_run_policy(self) -> ScheduleMissedRunPolicy:
        return self._missed_run_policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "name": self._name,
            "idDataConnector": self._id_data_connector,
            "enabled": self._enabled,
            "tracingEnabled": self._tracing_enabled,
            "schedule": self._schedule,
            "timezone": self._timezone,
            "overlapPolicy": self._overlap_policy.value,
            "missedRunPolicy": self._missed_run_policy.value,
        }

    def get_property(self, name: str) -> Any:
        return self._properties.get(name)


class TemporalEndpointConfig(CronEndpointConfig):
    def __init__(
        self,
        id: int,
        name: str,
        id_data_connector: int,
        temporal_execution_type: TemporalExecutionType,
        enabled: bool = False,
        tracing_enabled: bool = False,
        task_queue: str = "",
        schedule: str = "",
        schedule_id: str = "",
        timezone: str = "UTC",
        overlap_policy: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP,
        missed_run_policy: ScheduleMissedRunPolicy = ScheduleMissedRunPolicy.SKIP,
        workflow_execution_timeout: int = 0,
        activity_start_to_close_timeout: int = 0,
        activity_heartbeat_timeout: int = 0,
        maximum_attempts: int = 1,
        properties: Optional[dict[str, Any]] = None,
    ):
        super().__init__(
            id=id,
            name=name,
            id_data_connector=id_data_connector,
            enabled=enabled,
            tracing_enabled=tracing_enabled,
            schedule=schedule,
            timezone=timezone,
            overlap_policy=overlap_policy,
            missed_run_policy=missed_run_policy,
            properties=properties,
        )
        if (
            temporal_execution_type is TemporalExecutionType.ACTIVITY
            and activity_start_to_close_timeout < 1
        ):
            raise ValueError(
                "Temporal activity start-to-close timeout must be positive"
            )
        if maximum_attempts < 1:
            raise ValueError("Temporal maximum attempts must be positive")
        if not isinstance(temporal_execution_type, TemporalExecutionType):
            raise ValueError(
                "Temporal endpoint execution type must be Activity or Workflow"
            )
        self._task_queue = task_queue
        self._temporal_execution_type = temporal_execution_type
        self._schedule_id = schedule_id
        self._workflow_execution_timeout = workflow_execution_timeout
        self._activity_start_to_close_timeout = activity_start_to_close_timeout
        self._activity_heartbeat_timeout = activity_heartbeat_timeout
        self._maximum_attempts = maximum_attempts

    @property
    def task_queue(self) -> str:
        return self._task_queue

    @property
    def temporal_execution_type(self) -> TemporalExecutionType:
        return self._temporal_execution_type

    @property
    def schedule_id(self) -> str:
        return self._schedule_id

    @property
    def workflow_execution_timeout(self) -> int:
        return self._workflow_execution_timeout

    @property
    def activity_start_to_close_timeout(self) -> int:
        return self._activity_start_to_close_timeout

    @property
    def activity_heartbeat_timeout(self) -> int:
        return self._activity_heartbeat_timeout

    @property
    def maximum_attempts(self) -> int:
        return self._maximum_attempts

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "taskQueue": self._task_queue,
                "temporalExecutionType": self._temporal_execution_type.value,
                "scheduleId": self._schedule_id,
                "workflowExecutionTimeout": self._workflow_execution_timeout,
                "activityStartToCloseTimeout": self._activity_start_to_close_timeout,
                "activityHeartbeatTimeout": self._activity_heartbeat_timeout,
                "maximumAttempts": self._maximum_attempts,
            }
        )
        return result

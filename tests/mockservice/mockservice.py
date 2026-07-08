#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
import dataclasses
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel

from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.data_connector_implementation import DataConnectorImplementation
from pyservicelib_gorundebug.api.models.data_type import DataType
from pyservicelib_gorundebug.api.models.environment import Environment
from pyservicelib_gorundebug.api.models.http_method_type import HTTPMethodType
from pyservicelib_gorundebug.api.models.programming_language import ProgrammingLanguage
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.api.models.type_definition_format import TypeDefinitionFormat
from pyservicelib_gorundebug.datasink.localsink.custom import make_custom_endpoint_consumer
from pyservicelib_gorundebug.datasource.http.aiohttpds import (
    make_net_http_endpoint_consumer,
    HandlerData, ResultContext, StreamContext,
)
from pyservicelib_gorundebug.runtime.common import Consumer, Collect, TypedSinkStream, TypedInputStream
from pyservicelib_gorundebug.runtime.config import (
    ServiceAppConfig, ConfigSettings,
    ServiceConfig, StreamConfig, PoolConfig, LinkConfig, TypeConfig, ProjectSettings,
    HttpDataConnectorConfig, CustomDataConnectorConfig,
    HttpEndpointConfig, CustomEndpointConfig,
)
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment import ServiceDependency, ServiceEnvironment
from pyservicelib_gorundebug.runtime.environment.log import LogsEngine
from pyservicelib_gorundebug.runtime.environment.metrics import MetricsEngine
from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp, ServiceAppLoader, _deep_merge
from pyservicelib_gorundebug import transformation

# ── ID constants (mirrors Go's iota blocks) ───────────────────────────────────

_income_service_id = 1

_input_request_id = 1
_sink_id = 2
_input_request3_id = 3
_sink4_id = 4
_input_request_sync_id = 5
_sink_sync_id = 6

_http_server_conn_id = 1
_custom_sink_conn_id = 2

_data_endpoint_id = 1
_data3_endpoint_id = 2
_data_sync_endpoint_id = 3
_sink_endpoint_id = 4
_sink4_endpoint_id = 5
_sink_sync_endpoint_id = 6

_task_pool_name = "TaskPool"
_priority_task_pool_name = "PriorityTaskPool"

# ── Typed config (mirrors Go's Config struct) ──────────────────────────────────

@dataclass
class MockConfig:
    @dataclass
    class _Services:
        income_service: ServiceConfig

    @dataclass
    class _Streams:
        input_request: StreamConfig
        sink: StreamConfig
        input_request3: StreamConfig
        sink4: StreamConfig
        input_request_sync: StreamConfig
        sink_sync: StreamConfig

    @dataclass
    class _DataConnectors:
        http_server: HttpDataConnectorConfig
        custom_sink_connector: CustomDataConnectorConfig

    @dataclass
    class _Endpoints:
        data: HttpEndpointConfig
        data3: HttpEndpointConfig
        data_sync: HttpEndpointConfig
        sink: CustomEndpointConfig
        sink4: CustomEndpointConfig
        sink_sync: CustomEndpointConfig

    @dataclass
    class _Pools:
        task_pool: PoolConfig
        priority_task_pool: PoolConfig

    @dataclass
    class _Links:
        input_request_to_sink: LinkConfig
        input_request3_to_sink4: LinkConfig
        input_request_sync_to_sink_sync: LinkConfig

    @dataclass
    class _Types:
        request_data: TypeConfig
        int_type: TypeConfig
        map_type: TypeConfig

    services: _Services
    streams: _Streams
    data_connectors: _DataConnectors
    endpoints: _Endpoints
    pools: _Pools
    links: _Links
    types: _Types
    settings: ProjectSettings


# ── Config → named-map dict ────────────────────────────────────────────────────

def _camel(s: str) -> str:
    parts = s.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])


def _config_to_dict(cfg: MockConfig) -> dict[str, Any]:
    def _field_map(obj: Any) -> dict[str, Any]:
        return {
            _camel(f.name): getattr(obj, f.name).to_dict()
            for f in dataclasses.fields(obj)
        }
    return {
        "services": _field_map(cfg.services),
        "streams": _field_map(cfg.streams),
        "dataConnectors": _field_map(cfg.data_connectors),
        "endpoints": _field_map(cfg.endpoints),
        "pools": _field_map(cfg.pools),
        "links": _field_map(cfg.links),
        "types": _field_map(cfg.types),
        "settings": cfg.settings.to_dict(),
    }


def make_config() -> MockConfig:
    return MockConfig(
        services=MockConfig._Services(
            income_service=ServiceConfig(
                id=_income_service_id,
                name="IncomeService",
                color="#D2E5FF",
                grpcHost="localhost",
                grpcPort=9201,
                httpHost="localhost",
                httpPort=9091,
                metricsHandler="/metrics",
                shutdownTimeout=30000,
                statusHandler="/status",
                environment=Environment.Undefined,
                defaultCallSemantics=CallSemantics.TaskPool,
                defaultGrpcTimeout=0,
                programmingLanguage=ProgrammingLanguage.Python,
                modulePath="github.com/gorundebug/servicelib",
            ),
        ),
        streams=MockConfig._Streams(
            input_request=StreamConfig(
                id=_input_request_id, name="InputRequest", type=TransformationType.Input,
                idService=_income_service_id, idEndpoint=_data_endpoint_id,
                idSource=0, valueType="RequestData", xPos=-1, yPos=36,
            ),
            sink=StreamConfig(
                id=_sink_id, name="Sink", type=TransformationType.Sink,
                idService=_income_service_id, idEndpoint=_sink_endpoint_id,
                idSource=_input_request_id, valueType="error", xPos=-1285, yPos=-62,
            ),
            input_request3=StreamConfig(
                id=_input_request3_id, name="InputRequest3", type=TransformationType.Input,
                idService=_income_service_id, idEndpoint=_data3_endpoint_id,
                idSource=0, valueType="RequestData", xPos=-1, yPos=36,
            ),
            sink4=StreamConfig(
                id=_sink4_id, name="Sink4", type=TransformationType.Sink,
                idService=_income_service_id, idEndpoint=_sink4_endpoint_id,
                idSource=_input_request3_id, xPos=-1285, yPos=-62,
            ),
            input_request_sync=StreamConfig(
                id=_input_request_sync_id, name="InputRequestSync", type=TransformationType.Input,
                idService=_income_service_id, idEndpoint=_data_sync_endpoint_id,
                idSource=0, valueType="RequestData", xPos=-1, yPos=36,
            ),
            sink_sync=StreamConfig(
                id=_sink_sync_id, name="SinkSync", type=TransformationType.Sink,
                idService=_income_service_id, idEndpoint=_sink_sync_endpoint_id,
                idSource=_input_request_sync_id, xPos=-1285, yPos=-62,
            ),
        ),
        data_connectors=MockConfig._DataConnectors(
            http_server=HttpDataConnectorConfig(
                id=_http_server_conn_id, name="HttpServer",
                implementation=DataConnectorImplementation.aiohttp,
                host="localhost", port=8080,
            ),
            custom_sink_connector=CustomDataConnectorConfig(
                id=_custom_sink_conn_id, name="CustomSink",
                implementation=DataConnectorImplementation.Function,
            ),
        ),
        endpoints=MockConfig._Endpoints(
            data=HttpEndpointConfig(
                id=_data_endpoint_id, name="Data",
                id_data_connector=_http_server_conn_id,
                http_method_type=HTTPMethodType.POST, path="/data",
            ),
            data3=HttpEndpointConfig(
                id=_data3_endpoint_id, name="Data3",
                id_data_connector=_http_server_conn_id,
                http_method_type=HTTPMethodType.POST, path="/data3",
            ),
            data_sync=HttpEndpointConfig(
                id=_data_sync_endpoint_id, name="DataSync",
                id_data_connector=_http_server_conn_id,
                http_method_type=HTTPMethodType.POST, path="/dataSync",
            ),
            sink=CustomEndpointConfig(
                id=_sink_endpoint_id, name="Sink", id_data_connector=_custom_sink_conn_id,
            ),
            sink4=CustomEndpointConfig(
                id=_sink4_endpoint_id, name="Sink4", id_data_connector=_custom_sink_conn_id,
            ),
            sink_sync=CustomEndpointConfig(
                id=_sink_sync_endpoint_id, name="SinkSync", id_data_connector=_custom_sink_conn_id,
            ),
        ),
        pools=MockConfig._Pools(
            task_pool=PoolConfig(name=_task_pool_name, executorsCount=8),
            priority_task_pool=PoolConfig(name=_priority_task_pool_name, executorsCount=8),
        ),
        links=MockConfig._Links(
            input_request_to_sink=LinkConfig(  # type: ignore[call-arg]
                var_from=_input_request_id,  # pyright: ignore[reportCallIssue]
                to=_sink_id,
                callSemantics=CallSemantics.PriorityTaskPool,
                poolName=_priority_task_pool_name, priority=0,
            ),
            input_request3_to_sink4=LinkConfig(  # type: ignore[call-arg]
                var_from=_input_request3_id,  # pyright: ignore[reportCallIssue]
                to=_sink4_id,
                callSemantics=CallSemantics.TaskPool,
                poolName=_task_pool_name,
            ),
            input_request_sync_to_sink_sync=LinkConfig(  # type: ignore[call-arg]
                var_from=_input_request_sync_id,  # pyright: ignore[reportCallIssue]
                to=_sink_sync_id,
                callSemantics=CallSemantics.FunctionCall,
            ),
        ),
        types=MockConfig._Types(
            request_data=TypeConfig(
                name="RequestData", type=DataType.struct,
                definitionFormat=TypeDefinitionFormat.Protobuf,
            ),
            int_type=TypeConfig(name="IntType", type=DataType.int),
            map_type=TypeConfig(
                name="MapType", type=DataType.map,
                keyType="IntType", valueType="boolean",
            ),
        ),
        settings=ProjectSettings(name="MockService"),
    )


# ── MockServiceConfig ──────────────────────────────────────────────────────────

class RequestData(BaseModel):
    text: str = ""


class MockServiceConfig(ServiceAppConfig):
    def __init__(self, **data):
        super().__init__(**data)

    @classmethod
    def _load_config(cls, obj: dict[str, Any]) -> dict[str, Any]:
        # List format (old Python style): pass through directly.
        # Named-map format (Go style): merge with make_config() defaults and convert to lists.
        if isinstance(obj.get("services"), list):
            return super()._load_config(obj)
        base_dict: dict[str, Any] = _config_to_dict(make_config())
        merged = _deep_merge(base_dict, obj)
        list_obj: dict[str, Any] = {
            "services": list(merged.get("services", {}).values()),
            "streams": list(merged.get("streams", {}).values()),
            "dataConnectors": list(merged.get("dataConnectors", {}).values()),
            "endpoints": list(merged.get("endpoints", {}).values()),
            "pools": list(merged.get("pools", {}).values()),
            "links": list(merged.get("links", {}).values()),
            "types": list(merged.get("types", {}).values()),
            "settings": merged.get("settings", {}),
        }
        return super()._load_config(list_obj)

    @classmethod
    def load_config(cls, obj: dict[str, Any]) -> dict[str, Any]:
        return {}


class MockServiceDependency(ServiceDependency):
    async def get_logs_engine(self, env: ServiceEnvironment) -> Optional[LogsEngine]:
        return None

    async def get_metrics_engine(self, env: ServiceEnvironment) -> Optional[MetricsEngine]:
        return None


# ── HTTP handler ───────────────────────────────────────────────────────────────

class _RequestDataHttpHandler:
    async def begin_request(
        self,
        sc: StreamContext[RequestData, Any, Exception],
        data: HandlerData,
    ) -> tuple[HandlerData, None]:
        return data, None

    async def consume_message(
        self,
        sc: StreamContext[RequestData, Any, Exception],
        handler_state: None,
        data: HandlerData,
        result_ctx: ResultContext[None, RequestData, Any, Exception],
    ) -> None:
        result_ctx.done()
        body = await data.request.read()
        rd = RequestData.model_validate_json(body)
        await sc.collect(rd)

    def get_message_id(
        self,
        sc: StreamContext[RequestData, Any, Exception],
        handler_state: None,
        value: Any,
    ) -> str:
        return ""

    async def end_request(
        self,
        sc: StreamContext[RequestData, Any, Exception],
        err: Optional[Exception],
        handler_state: None,
        data: HandlerData,
    ) -> None:
        pass


# ── Endpoint sinks ─────────────────────────────────────────────────────────────

class EndpointSink:
    def __init__(self, mock_service: "MockService"):
        self._mock_service = mock_service

    def get_stream_id(self, ctx: Context, value: RequestData) -> str:
        return ""

    async def begin_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
    ) -> tuple[Context, None]:
        return ctx, None

    async def consume_message(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        handler_state: None, value: RequestData, result_stream: Collect[Exception],
    ) -> None:
        self._mock_service.request_data = value
        self._mock_service.request_data_event.set()
        self._mock_service.request_data_event.clear()

    async def end_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        err: Optional[Exception], handler_state: None,
    ) -> None:
        pass


class EndpointSink4:
    def __init__(self, mock_service: "MockService"):
        self._mock_service = mock_service

    def get_stream_id(self, ctx: Context, value: RequestData) -> str:
        return ""

    async def begin_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
    ) -> tuple[Context, None]:
        return ctx, None

    async def consume_message(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        handler_state: None, value: RequestData, result_stream: Collect[Exception],
    ) -> None:
        self._mock_service.request_data = value
        self._mock_service.request_data_event.set()
        self._mock_service.request_data_event.clear()

    async def end_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        err: Optional[Exception], handler_state: None,
    ) -> None:
        pass


class EndpointSinkSync:
    def __init__(self, mock_service: "MockService"):
        self._mock_service = mock_service

    def get_stream_id(self, ctx: Context, value: RequestData) -> str:
        return ""

    async def begin_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
    ) -> tuple[Context, None]:
        return ctx, None

    async def consume_message(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        handler_state: None, value: RequestData, result_stream: Collect[Exception],
    ) -> None:
        self._mock_service.request_data = value
        self._mock_service.request_data_event.set()
        self._mock_service.request_data_event.clear()

    async def end_request(
        self, ctx: Context, stream: TypedSinkStream[RequestData, Exception],
        err: Optional[Exception], handler_state: None,
    ) -> None:
        pass


# ── MockService ────────────────────────────────────────────────────────────────

class MockService(ServiceApp):

    request_data: Optional[RequestData]
    request_data_event: asyncio.Event

    _sink: TypedSinkStream[RequestData, Exception]
    _input_request: TypedInputStream[RequestData, Any, Exception]
    _sink4: TypedSinkStream[RequestData, Exception]
    _input_request3: TypedInputStream[RequestData, Any, Exception]
    _sink_sync: TypedSinkStream[RequestData, Exception]
    _input_request_sync: TypedInputStream[RequestData, Any, Exception]
    _input_request_source: Consumer[RequestData]
    _input_request_source_sync: Consumer[RequestData]
    _stream_sink: Consumer[RequestData]
    _stream4_sink: Consumer[RequestData]
    _stream_sink_sync: Consumer[RequestData]

    def __init__(self):
        super().__init__()
        self.request_data = None
        self.request_data_event = asyncio.Event()

    def get_config(self) -> MockServiceConfig:
        return self.config  # type: ignore[return-value]

    async def build_runtime(self) -> None:
        cfg = self.get_config()
        http_handler = _RequestDataHttpHandler()
        endpoint_sink = EndpointSink(self)
        endpoint_sink4 = EndpointSink4(self)
        endpoint_sink_sync = EndpointSinkSync(self)

        input_request_cfg = cfg.get_input_stream_config("InputRequest")
        input_request3_cfg = cfg.get_input_stream_config("InputRequest3")
        input_request_sync_cfg = cfg.get_input_stream_config("InputRequestSync")
        sink_cfg = cfg.get_sink_stream_config("Sink")
        sink4_cfg = cfg.get_sink_stream_config("Sink4")
        sink_sync_cfg = cfg.get_sink_stream_config("SinkSync")
        assert input_request_cfg is not None
        assert input_request3_cfg is not None
        assert input_request_sync_cfg is not None
        assert sink_cfg is not None
        assert sink4_cfg is not None
        assert sink_sync_cfg is not None

        self._input_request = transformation.Input[RequestData, Any, Exception](
            input_request_cfg, self)
        self._input_request3 = transformation.Input[RequestData, Any, Exception](
            input_request3_cfg, self)
        self._input_request_sync = transformation.Input[RequestData, Any, Exception](
            input_request_sync_cfg, self)

        self._input_request_source = make_net_http_endpoint_consumer(
            self._input_request, http_handler)
        self._input_request_source_sync = make_net_http_endpoint_consumer(
            self._input_request_sync, http_handler)

        self._sink = transformation.Sink[RequestData, Exception](
            sink_cfg, self._input_request)
        self._sink4 = transformation.Sink[RequestData, Exception](
            sink4_cfg, self._input_request3)
        self._sink_sync = transformation.Sink[RequestData, Exception](
            sink_sync_cfg, self._input_request_sync)

        self._stream_sink = make_custom_endpoint_consumer(self._sink, endpoint_sink)
        self._stream4_sink = make_custom_endpoint_consumer(self._sink4, endpoint_sink4)
        self._stream_sink_sync = make_custom_endpoint_consumer(self._sink_sync, endpoint_sink_sync)

    async def start_service(self, ctx: Context) -> None:
        await self.build_runtime()
        await self.start(ctx)

    async def stop_service(self, ctx: Context) -> None:
        await self.stop(ctx)


# ── TestEnv ────────────────────────────────────────────────────────────────────

class TestEnv:
    service: MockService

    def __init__(self, service: MockService):
        self.service = service


async def setup(config_dir: Optional[str] = None) -> TestEnv:
    if config_dir is None:
        config_dir = str(Path(__file__).parent / "config")
    sys.argv = [sys.argv[0],
                "--config", os.path.join(config_dir, "config.yaml"),
                "--overrides", os.path.join(config_dir, "overrides.yaml")]

    service = await ServiceAppLoader[MockService, MockServiceConfig]().load(
        "IncomeService", MockServiceDependency(), ConfigSettings())

    ctx = Context()
    await service.start_service(ctx)
    return TestEnv(service=service)


async def teardown(env: TestEnv) -> None:
    ctx = Context()
    await env.service.stop_service(ctx)

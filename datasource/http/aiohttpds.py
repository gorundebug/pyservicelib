#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from abc import abstractmethod, ABC
import aiohttp.log
from aiohttp import web
import logging
from typing import cast, Optional, Callable, Awaitable, Union, Any, get_origin
from multidict import MultiMapping
from pydantic import BaseModel

from pyservicelib.runtime import TypedInputStream
from pyservicelib.runtime.common import InputEndpoint, ServiceExecutionEnvironment
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint

def _flatten_params(*param_dicts: MultiMapping) -> dict[str, Union[Any, list[Any]]]:
    result: dict[str, Union[Any, list[Any]]] = {}
    for params in param_dicts:
        for key in params:
            values = params.getall(key)
            if key in result:
                if isinstance(result[key], list):
                    result[key].extend(values)
                else:
                    result[key] = [result[key]] + values
            else:
                result[key] = values[0] if len(values) == 1 else values
    return result

class HTTPEndpointRequestData:
    _request: web.Request
    _body: Optional[bytes]
    _json: Optional[bytes]
    _form: Optional[dict[str, Union[Any, list[Any]]]]

    def __init__(self, request: web.Request):
        self._request = request
        self._body = None
        self._form = None
        self._json = None

    @property
    async def body(self) -> bytes:
        if self._body is None:
            self._body = await self._request.read()
        return self._body

    @property
    async def json(self) -> Any:
        if self._json is None:
            self._json = await self._request.json()
        return self._json

    @property
    async def form(self):
        if self._form is None:
            self._form = _flatten_params(self._request.query, await self._request.post())
        return self._form


class AIOHttpEndpoint(DataSourceEndpoint):
    def __init__(self, datasource: "AIOHttpDataSource", id_endpoint: int):
        cfg = datasource.environment.config.get_endpoint_config_by_id(id_endpoint)
        if cfg.method is None:
            raise ValueError(f"Method property can not be None for endpoint '{cfg.name}'")
        if cfg.path is None:
            raise ValueError(f"Path property can not be None for endpoint '{cfg.name}'")
        if cfg.format is None:
            raise ValueError(f"Format property can not be None for endpoint '{cfg.name}'")
        elif (cfg.method == "POST" and cfg.format not in ["json", "form"] or
              cfg.method == "GET" and cfg.format != "form"):
            raise ValueError(f"Format property has invalid value '{cfg.format}' for endpoint '{cfg.name}'")

        super().__init__(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_handler(cfg.method, cfg.path, self.handler)

    async def handler(self, request: web.Request) -> web.Response:
        request_data = HTTPEndpointRequestData(request)
        try:
            for ec in self.endpoint_consumers:
                await cast(AIOHttpEndpointConsumer, ec).endpoint_request(request_data)
        except web.HTTPException as e:
            return web.Response(text=f"HTTP error occurred: {e}", status=400)
        except ValueError as e:
            return web.Response(text=f"Value error: {e}", status=400)
        except Exception as e:
            return web.Response(text=f"An unexpected error occurred: {e}", status=500)

        return web.Response()

    async def start(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(AIOHttpEndpointConsumer, ec).start(ctx)

    async def stop(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(AIOHttpEndpointConsumer, ec).stop(ctx)


class AIOHttpDataSource(InputDataSource):

    _app: web.Application
    _runner: Optional[web.AppRunner]
    _site: Optional[web.TCPSite]

    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)

        self._app = web.Application(
            logger=cast(logging.Logger, env.log.native_logger) if isinstance(env.log.native_logger,
                                                                             logging.Logger) else aiohttp.log.web_logger)
        self._runner = None
        self._site = None

    async def start(self, ctx: Context) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        if self.data_connector.host is None:
            raise ValueError(f"Host property can not be None for http data source '{self.data_connector.name}'")
        if self.data_connector.port is None:
            raise ValueError(f"Port property can not be None for http data source '{self.data_connector.name}'")
        self._site = web.TCPSite(runner=self._runner, host=self.data_connector.host, port=self.data_connector.port)
        await self._site.start()

        for ep in self.endpoints:
            await cast(AIOHttpEndpoint, ep).start(ctx)


    async def stop(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast(AIOHttpEndpoint, ep).stop(ctx)

        if self._runner is not None:
            await self._runner.cleanup()

    def add_handler(self, method: str, path: str, handler: Callable[[web.Request], Awaitable[web.Response]]) -> None:
        self._app.router.add_route(method=method, path=path, handler=handler)


class EndpointRequestConsumer(ABC):

    @abstractmethod
    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
       pass


class JsonRequestEndpointConsumer[T](EndpointRequestConsumer):
    _endpoint_consumer: "AIOHttpEndpointConsumer[T]"

    def __init__(self, endpoint_consumer: "AIOHttpEndpointConsumer[T]"):
        self._endpoint_consumer = endpoint_consumer

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        json = await request_data.json
        if self._endpoint_consumer.reader is None:
            value = cast(T, cast(BaseModel, self._endpoint_consumer.value_type).model_validate_json(json))
        else:
            value = self._endpoint_consumer.reader.from_dict(json)
        await self._endpoint_consumer.consume(value)


class FormRequestEndpointConsumer[T](EndpointRequestConsumer):
    _endpoint_consumer: "AIOHttpEndpointConsumer[T]"

    def __init__(self, endpoint_consumer: "AIOHttpEndpointConsumer[T]"):
        self._endpoint_consumer = endpoint_consumer

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        form = await request_data.form
        if self._endpoint_consumer.reader is None:
            value = self._endpoint_consumer.value_type(**form)
        else:
            value = self._endpoint_consumer.reader.from_dict(form)
        await self._endpoint_consumer.consume(value)


class AIOHttpEndpointConsumer[T](DataSourceEndpointConsumer[T]):
    _orig_type: type
    _request_endpoint_consumer: EndpointRequestConsumer

    def __init__(self, input_stream: TypedInputStream[T]):
        endpoint = AIOHttpEndpointConsumer.get_aiohttp_datasource_endpoint(input_stream.endpoint_id,
                                                                            input_stream.environment)
        super().__init__(endpoint=endpoint, input_stream=input_stream)
        endpoint.add_endpoint_consumer(self)
        if self.endpoint.config.format == "json":
            self._request_endpoint_consumer = JsonRequestEndpointConsumer(self)
        else:
            self._request_endpoint_consumer = FormRequestEndpointConsumer(self)

    @property
    def value_type(self) -> type:
        return self._orig_type

    async def endpoint_request(self, request_data: HTTPEndpointRequestData):
        await self._request_endpoint_consumer.endpoint_request(request_data)

    async def start(self, ctx: Context):
        genetic_type = self.__orig_class__.__args__[0] #type: ignore[attr-defined]
        self._orig_type = get_origin(genetic_type)
        if self._orig_type is None:
            self._orig_type = genetic_type
        if not issubclass(self._orig_type, BaseModel):
            raise ValueError(f"Invalid type value for endpoint {self.endpoint.name}. Use pydantic define custom reader.")

    async def stop(self, ctx: Context):
        pass

    @classmethod
    def get_aiohttp_datasource(cls, id_connector: int, env: ServiceExecutionEnvironment) -> AIOHttpDataSource:
        datasource = env.get_datasource(id_connector)
        if datasource is not None:
            return cast(AIOHttpDataSource, datasource)
        cfg = env.config.get_data_connector_by_id(id_connector)
        aiohttp_datasource = AIOHttpDataSource(cfg.id, env)
        env.add_datasource(aiohttp_datasource)
        return aiohttp_datasource

    @classmethod
    def get_aiohttp_datasource_endpoint(cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> InputEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        datasource = cls.get_aiohttp_datasource(cfg.id_data_connector, env)
        endpoint = datasource.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        aiohttp_endpoint = AIOHttpEndpoint(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_endpoint(aiohttp_endpoint)
        return aiohttp_endpoint

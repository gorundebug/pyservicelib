#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import asyncio
from aiohttp import web
import logging
from typing import cast

from pyservicelib.runtime import TypedInputStream
from pyservicelib.runtime.common import InputEndpoint, ServiceExecutionEnvironment, DataSource
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.datasource import DataSourceEndpointConsumer, InputDataSource, DataSourceEndpoint


class AIOHttpEndpoint(DataSourceEndpoint):
    def __init__(self, datasource: DataSource, id_endpoint: int):
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)


class AIOHttpDataSource(InputDataSource):
    def __init__(self, connector_id: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=connector_id, env=env)

        self.app = web.Application(
            logger=cast(logging.Logger, env.log.native_logger) if isinstance(env.log.native_logger,
                                                                             logging.Logger) else None)

    async def start(self, ctx: Context) -> None:
        pass

    async def stop(self, ctx: Context) -> None:
        pass


class AIOHttpEndpointConsumer[T](DataSourceEndpointConsumer[T]):

    def __init__(self, input_stream: TypedInputStream[T]):
        endpoint = AIOHttpEndpointConsumer._get_aiohttp_datasource_endpoint(input_stream.endpoint_id,
                                                                            input_stream.environment)
        super().__init__(endpoint=endpoint, input_stream=input_stream)

    @classmethod
    def _get_aiohttp_datasource(cls, id_connector: int, env: ServiceExecutionEnvironment) -> DataSource:
        datasource = env.get_datasource(id_connector)
        if datasource is not None:
            return datasource
        cfg = env.config.get_data_connector_by_id(id_connector)
        aiohttp_datasource = AIOHttpDataSource(cfg.id, env)
        env.add_datasource(aiohttp_datasource)
        return aiohttp_datasource

    @classmethod
    def _get_aiohttp_datasource_endpoint(cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> InputEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        datasource = cls._get_aiohttp_datasource(cfg.id_data_connector, env)
        endpoint = datasource.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        aiohttp_endpoint = AIOHttpEndpoint(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_endpoint(aiohttp_endpoint)
        return aiohttp_endpoint

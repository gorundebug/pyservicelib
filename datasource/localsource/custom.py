#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import asyncio
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Protocol, cast, Any, Optional

from pyservicelib.runtime import Consume, ServiceExecutionEnvironment
from pyservicelib.runtime.common import InputEndpoint, TypedInputStream, DataSource
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.datasource import InputDataSource, DataSourceEndpoint, DataSourceEndpointConsumer


class DataProducer[T](Protocol):

    async def start(self, ctx: Context, consumer: Consume[T]):
        ...

    async def stop(self, ctx: Context):
        ...


class CustomEndpointConsumer(ABC):

    @abstractmethod
    async def start(self, ctx:Context):
        pass

    @abstractmethod
    async def stop(self, ctx:Context):
        pass


class CustomInputEndpoint(DataSourceEndpoint):
    _delay: timedelta

    def __init__(self, datasource: DataSource, id_endpoint: int):
        super().__init__(datasource=datasource, id_endpoint=id_endpoint)

    async def start(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(CustomEndpointConsumer, ec).start(ctx)

    async def stop(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(CustomEndpointConsumer, ec).stop(ctx)

    async def next_message(self):
        total_seconds = self._delay.total_seconds()
        if total_seconds > 0:
            await asyncio.sleep(total_seconds)


class CustomDataSource(InputDataSource):

    def __init__(self, id_connector: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=id_connector, env=env)

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast(CustomInputEndpoint, ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_tasks: list[asyncio.Task[Any]] = []
        for ep in self.endpoints:
            stop_tasks.append(asyncio.create_task(cast(CustomInputEndpoint, ep).stop(ctx)))
        try:
            await asyncio.wait_for(asyncio.gather(*stop_tasks), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            self.environment.log.warning(f"Custom data source '{self.name}' stopped by timeout.")


class TypedCustomEndpointConsumer[T](DataSourceEndpointConsumer[T], CustomEndpointConsumer):
    _data_producer: DataProducer[T]
    _runner_task: Optional[asyncio.Task[Any]]

    def __init__(self, input_stream: TypedInputStream[T], data_producer: DataProducer[T]):
        env = input_stream.environment
        endpoint = TypedCustomEndpointConsumer._get_custom_datasource_endpoint(
            id_endpoint=input_stream.endpoint_id, env=env)
        super().__init__(endpoint=endpoint, input_stream=input_stream)
        self._data_producer = data_producer
        self._runner_task = None
        endpoint.add_endpoint_consumer(self)

    async def _runner(self, ctx: Context):
        await self._data_producer.start(ctx, self)

    async def start(self, ctx: Context):
        self._runner_task = asyncio.create_task(self._runner(ctx))

    async def stop(self, ctx: Context):
        if self._runner_task is not None:
            try:
                await asyncio.wait_for(asyncio.gather( self._data_producer.stop(ctx), self._runner_task), timeout=ctx.time_left)
            except asyncio.TimeoutError:
                self._input_stream.environment.log.warning(f"Custom data source endpoint '{self.endpoint.name}' for stream '{self._input_stream.name}' stopped by timeout.")

    async def consume(self, value: T) -> None:
        await cast(CustomInputEndpoint, self.endpoint).next_message()
        await super().consume(value)

    @classmethod
    def _get_custom_datasource(cls, id_connector: int, env: ServiceExecutionEnvironment) -> DataSource:
        datasource = env.get_datasource(id_connector)
        if datasource is not None:
            return datasource
        cfg = env.config.get_data_connector_by_id(id_connector)
        custom_datasource = CustomDataSource(cfg.id, env)
        env.add_datasource(custom_datasource)
        return custom_datasource

    @classmethod
    def _get_custom_datasource_endpoint(cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> InputEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        datasource = cls._get_custom_datasource(cfg.id_data_connector, env)
        endpoint = datasource.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        custom_endpoint = CustomInputEndpoint(datasource=datasource, id_endpoint=id_endpoint)
        datasource.add_endpoint(custom_endpoint)
        return custom_endpoint

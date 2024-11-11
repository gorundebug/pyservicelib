#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
import asyncio
from typing import Protocol, cast, Any
from abc import ABC, abstractmethod

from pyservicelib.runtime import TypedSinkStream, ServiceExecutionEnvironment
from pyservicelib.runtime.common import DataSink, SinkEndpoint
from pyservicelib.runtime.context import Context
from pyservicelib.runtime.datasink import OutputDataSink, DataSinkEndpointConsumer, DataSinkEndpoint


class DataConsumer[T](Protocol):

    async def start(self, ctx: Context):
        ...

    async def stop(self, ctx: Context):
        ...

    async def consume(self, value: T):
        ...


class CustomEndpointConsumer(ABC):

    @abstractmethod
    async def start(self, ctx:Context):
        pass

    @abstractmethod
    async def stop(self, ctx:Context):
        pass


class CustomSinkEndpoint(DataSinkEndpoint):

    def __init__(self, data_sink: DataSink, id_endpoint: int):
        super().__init__(data_sink=data_sink, id_endpoint=id_endpoint)

    async def start(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(CustomEndpointConsumer, ec).start(ctx)

    async def stop(self, ctx: Context):
        for ec in self.endpoint_consumers:
            await cast(CustomEndpointConsumer, ec).stop(ctx)


class CustomDataSink(OutputDataSink):
    def __init__(self, id_connector: int, env: ServiceExecutionEnvironment):
        super().__init__(connector_id=id_connector, env=env)

    async def start(self, ctx: Context) -> None:
        for ep in self.endpoints:
            await cast(CustomSinkEndpoint, ep).start(ctx)

    async def stop(self, ctx: Context) -> None:
        stop_tasks: list[asyncio.Task[Any]] = []
        for ep in self.endpoints:
            stop_tasks.append(asyncio.create_task(cast(CustomSinkEndpoint, ep).stop(ctx)))
        try:
            await asyncio.wait_for(asyncio.gather(*stop_tasks), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            self.environment.log.warning(f"Custom data sink '{self.name}' stopped by timeout.")


class TypedCustomEndpointConsumer[T](DataSinkEndpointConsumer[T], CustomEndpointConsumer):
    _data_consumer: DataConsumer[T]

    def __init__(self, sink_stream: TypedSinkStream[T], _data_consumer: DataConsumer[T]):
        env = sink_stream.environment
        endpoint = TypedCustomEndpointConsumer._get_custom_datasink_endpoint(
            id_endpoint=sink_stream.endpoint_id, env=env)
        super().__init__(endpoint=endpoint, sink_stream=sink_stream)
        endpoint.add_endpoint_consumer(self)

    async def start(self, ctx: Context):
        await self._data_consumer.start(ctx)

    async def stop(self, ctx: Context):
        try:
            await asyncio.wait_for(asyncio.gather(self._data_consumer.stop(ctx), ), timeout=ctx.time_left)
        except asyncio.TimeoutError:
            self._sink_stream.environment.log.warning(f"Custom data sink endpoint '{self.endpoint.name}' for stream '{self._sink_stream.name}' stopped by timeout.")

    async def consume(self, value: T) -> None:
        await self._data_consumer.consume(value)

    @classmethod
    def _get_custom_datasink(cls, id_connector: int, env: ServiceExecutionEnvironment) -> DataSink:
        datasink = env.get_datasink(id_connector)
        if datasink is not None:
            return datasink
        cfg = env.config.get_data_connector_by_id(id_connector)
        custom_datasink = CustomDataSink(cfg.id, env)
        env.add_datasink(custom_datasink)
        return custom_datasink

    @classmethod
    def _get_custom_datasink_endpoint(cls, id_endpoint: int, env: ServiceExecutionEnvironment) -> SinkEndpoint:
        cfg = env.config.get_endpoint_config_by_id(id_endpoint)
        data_sink = cls._get_custom_datasink(cfg.id_data_connector, env)
        endpoint = data_sink.get_endpoint(id_endpoint)
        if endpoint is not None:
            return endpoint
        custom_endpoint = CustomSinkEndpoint(data_sink=data_sink, id_endpoint=id_endpoint)
        data_sink.add_endpoint(custom_endpoint)
        return custom_endpoint
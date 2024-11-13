#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from .common import TypedStream, TypedConsumedStream, StreamConsumer, Stream, ServiceExecutionEnvironment
from .config import StreamConfig

class MergeLink[T](Stream, StreamConsumer[T]):
    _merge_stream: "MergeStream[T]"
    _source: TypedStream[T]
    _index: int

    def __init__(self, merge_stream: "MergeStream[T]", index: int,
                 stream: TypedStream[T]):
        self._merge_stream = merge_stream
        self._source = stream
        self._index = index
        stream.consumer = self

    @property
    def name(self) -> str:
        return self._merge_stream.name

    @property
    def transformation_name(self) -> str:
        return self._merge_stream.transformation_name

    @property
    def type_name(self) -> str:
        return self._merge_stream.type_name

    @property
    def id(self) -> int:
        return self._merge_stream.id

    @property
    def config(self) -> StreamConfig:
        return self._merge_stream.config

    @property
    def environment(self) -> ServiceExecutionEnvironment:
        return self._merge_stream.environment

    @property
    def consumers(self) -> list[Stream]:
        return self._merge_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._merge_stream

    async def consume(self, value: T) -> None:
        await self._merge_stream.consume(value)


class MergeStream[T](TypedConsumedStream[T]):
    _links: list[MergeLink[T]]

    def __init__(self, name: str, stream: TypedStream[T], *streams: TypedStream[T]):
        cfg = stream.environment.config.get_stream_config_by_name(name)
        if cfg is None:
            raise ValueError(f"MergeStream configuration names '{name}' not found")

        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)
        self._links = [MergeLink(self, 0, stream)]

        for i, s in enumerate(streams):
            self._links.append(MergeLink(self, i+1, stream))


    async def consume(self, value: T) -> None:
        if self._consumer is not None:
            await self._consumer.consume(value)



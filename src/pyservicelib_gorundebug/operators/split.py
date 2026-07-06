#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from ..runtime.common import TypedStream, TypedConsumedStream, TypedSplitStream, Stream
from ..runtime.config.stream_types import SplitStreamConfig


class SplitLink[T](TypedConsumedStream[T]):
    _split_stream: "SplitStream[T]"
    _index: int

    def __init__(self, split_stream: "SplitStream[T]", index: int):
        super().__init__(split_stream.id, split_stream.environment, split_stream.serde)
        self._split_stream = split_stream
        self._index = index

    @property
    def name(self) -> str:
        return f"{self._split_stream.name}SplitLink{self._index}"

    @property
    def consumers(self) -> list[Stream]:
        return self._split_stream.consumers

    @property
    def stream(self) -> Stream:
        return self._split_stream

    async def consume(self, value: T) -> None:
        if self._caller is not None:
            await self._caller.consume(value)


class SplitStream[T](TypedSplitStream[T]):
    _links: list[SplitLink[T]]
    _source: TypedStream[T]

    def __init__(self, cfg: SplitStreamConfig, stream: TypedStream[T]):
        super().__init__(stream_id=cfg.id, env=stream.environment, serde=stream.serde)
        self._source = stream
        self._links = []
        stream.consumer = self

    async def consume(self, value: T) -> None:
        for link in self._links:
            await link.consume(value)

    @property
    def consumers(self) -> list[Stream]:
        consumers: list[Stream] = []
        for link in self._links:
            if link.consumer is None:
                raise ValueError(f"SplitStream '{self.name}' does not have a consumer for all split streams")
            consumers.append(link.consumer.stream)
        return consumers

    def build(self) -> None:
        for i, link in enumerate(self._links):
            if link.consumer is None:
                raise ValueError(f"SplitStream '{self.name}' does not have a consumer for the link with index {i}")

    def add_stream(self) -> TypedConsumedStream[T]:
        index = len(self._links)
        link = SplitLink(self, index)
        self._links.append(link)
        return link

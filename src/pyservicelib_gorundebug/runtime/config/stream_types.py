#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
from __future__ import annotations

from typing import Optional

from ...api.models.join_storage_type import JoinStorageType
from ...api.models.join_type import JoinType
from .config import StreamConfig


class _TypedStreamConfig:
    __slots__ = ('_cfg',)

    def __init__(self, cfg: StreamConfig) -> None:
        self._cfg = cfg

    @property
    def id(self) -> int:
        return self._cfg.id

    @property
    def name(self) -> str:
        return self._cfg.name

    @property
    def pipeline(self) -> Optional[str]:
        return self._cfg.pipeline

    @property
    def id_service(self) -> int:
        return self._cfg.id_service

    @property
    def id_source(self) -> int:
        return self._cfg.id_source

    @property
    def id_sources(self) -> list[int]:
        return self._cfg.id_sources or []

    @property
    def raw_config(self) -> StreamConfig:
        return self._cfg

    def _require_str(self, attr: str, label: str) -> str:
        v = getattr(self._cfg, attr, None)
        if v is None:
            raise ValueError(f"{type(self).__name__} '{self.name}': {label} is required")
        return v

    def _require_int(self, attr: str, label: str) -> int:
        v = getattr(self._cfg, attr, None)
        if v is None:
            raise ValueError(f"{type(self).__name__} '{self.name}': {label} is required")
        return v


class InputStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')

    @property
    def id_endpoint(self) -> int:
        return self._require_int('id_endpoint', 'id_endpoint')


class MapStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class FilterStreamConfig(_TypedStreamConfig):
    pass


class FlatMapStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class FlatMapIterableStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class JoinStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')

    @property
    def join_type(self) -> JoinType:
        v = self._cfg.join_type
        return v if v is not None else JoinType.Inner

    @property
    def join_storage(self) -> JoinStorageType:
        v = self._cfg.join_storage
        return v if v is not None else JoinStorageType.HashMap

    @property
    def ttl(self) -> int:
        v = self._cfg.ttl
        return v if v is not None else 0

    @property
    def renew_ttl(self) -> bool:
        v = self._cfg.renew_ttl
        return v if v is not None else False


class MultiJoinStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')

    @property
    def join_storage(self) -> JoinStorageType:
        v = self._cfg.join_storage
        return v if v is not None else JoinStorageType.HashMap

    @property
    def ttl(self) -> int:
        v = self._cfg.ttl
        return v if v is not None else 0

    @property
    def renew_ttl(self) -> bool:
        v = self._cfg.renew_ttl
        return v if v is not None else False


class ProcessStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class KeyByStreamConfig(_TypedStreamConfig):
    @property
    def key_type(self) -> str:
        return self._require_str('key_type', 'key_type')

    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class MergeStreamConfig(_TypedStreamConfig):
    pass


class SplitStreamConfig(_TypedStreamConfig):
    pass


class DelayStreamConfig(_TypedStreamConfig):
    pass


class CaseStreamConfig(_TypedStreamConfig):
    pass


class WhenStreamConfig(_TypedStreamConfig):
    @property
    def value_type(self) -> str:
        return self._require_str('value_type', 'value_type')


class SinkStreamConfig(_TypedStreamConfig):
    @property
    def id_endpoint(self) -> int:
        return self._require_int('id_endpoint', 'id_endpoint')

    @property
    def value_type(self) -> Optional[str]:
        return getattr(self._cfg, 'value_type', None)


class CycleLinkStreamConfig(_TypedStreamConfig):
    pass

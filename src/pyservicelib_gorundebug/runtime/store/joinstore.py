#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Hashable

from ..common import ServiceEnvironment
from ...api.models.join_storage_type import JoinStorageType
from .storage import JoinStorageConfig, JoinStorage
from .hashmap import make_hashmap_storage

class JoinStorageFactory[K: Hashable]:

    @classmethod
    def make_storage(cls, storage_type: JoinStorageType, env: ServiceEnvironment, cfg: JoinStorageConfig) -> JoinStorage[K]:
        if storage_type == JoinStorageType.HashMap:
            return make_hashmap_storage(env, cfg)
        raise ValueError(f"Invalid storage type {storage_type} in JoinStorageFactory make_storage")
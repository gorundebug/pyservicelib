#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from typing import Hashable

from pyservicelib.runtime.environment import ServiceEnvironment
from pyservicelib.api.models.join_storage_type import JoinStorageType
from pyservicelib.runtime.store.storage import JoinStorageConfig
from pyservicelib.runtime.store.hashmap import make_hashmap_storage

class JoinStorageFactory[K: Hashable]:

    @classmethod
    def make_storage(cls, storage_type: JoinStorageType, env: ServiceEnvironment, cfg: JoinStorageConfig):
        if storage_type == JoinStorageType.HashMap:
            return make_hashmap_storage(env, cfg)
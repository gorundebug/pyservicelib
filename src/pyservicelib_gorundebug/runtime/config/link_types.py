#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from abc import ABC, abstractmethod
from ...api.models.call_semantics import CallSemantics


class CallSemanticsConfig(ABC):

    @property
    @abstractmethod
    def call_semantics(self) -> CallSemantics:
        pass


class FunctionCallSemanticsConfig(CallSemanticsConfig):

    @property
    def call_semantics(self) -> CallSemantics:
        return CallSemantics.FunctionCall


class TaskPoolCallSemanticsConfig(CallSemanticsConfig):

    def __init__(self, pool_name: str):
        self._pool_name = pool_name

    @property
    def pool_name(self) -> str:
        return self._pool_name

    @property
    def call_semantics(self) -> CallSemantics:
        return CallSemantics.TaskPool


class PriorityTaskPoolCallSemanticsConfig(CallSemanticsConfig):

    def __init__(self, pool_name: str, priority: int):
        self._pool_name = pool_name
        self._priority = priority

    @property
    def pool_name(self) -> str:
        return self._pool_name

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def call_semantics(self) -> CallSemantics:
        return CallSemantics.PriorityTaskPool


class ParallelCallSemanticsConfig(CallSemanticsConfig):

    @property
    def call_semantics(self) -> CallSemantics:
        return CallSemantics.ParallelCall

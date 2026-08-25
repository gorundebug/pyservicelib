from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar


_recording_policy: ContextVar[Callable[[], bool] | None] = ContextVar(
    "_runtime_recording_policy", default=None
)


def recording_enabled() -> bool:
    policy = _recording_policy.get()
    return policy() if policy is not None else True


@contextmanager
def recording_policy_scope(policy: Callable[[], bool]) -> Iterator[None]:
    token = _recording_policy.set(policy)
    try:
        yield
    finally:
        _recording_policy.reset(token)

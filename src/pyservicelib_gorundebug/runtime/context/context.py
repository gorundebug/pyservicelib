#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from datetime import timedelta
from collections.abc import Callable
import time
from typing import Optional


class Context:
    __deadline: float

    def __init__(
        self,
        timeout: Optional[timedelta] = None,
        *,
        _parent: Optional["Context"] = None,
        _deadline: Optional[float] = None,
    ):
        self.__parent = _parent
        # Keep cancellation as deterministic in-memory state. Context is also
        # constructed inside Temporal Workflow sandboxes, where threading
        # synchronization primitives are deliberately unavailable. Ordinary
        # maker groups use this flag cooperatively between Python threads.
        self.__cancelled = False
        self.__cancel_callbacks: dict[object, Callable[[], None]] = {}
        if _deadline is not None:
            self.__deadline = _deadline
        elif timeout is None:
            self.__deadline = float('inf')
        else:
            self.__deadline = time.perf_counter() + timeout.total_seconds()

    @property
    def is_expired(self) -> bool:
        return self.cancelled or time.perf_counter() >= self.__deadline

    @property
    def cancelled(self) -> bool:
        return self.__cancelled or (
            self.__parent is not None and self.__parent.cancelled
        )

    def cancel(self) -> None:
        self.__cancelled = True
        for callback in tuple(self.__cancel_callbacks.values()):
            callback()

    def add_cancel_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Notify once on local/ancestor cancellation; return an unsubscribe function.

        Callbacks run synchronously on the cancelling thread and must not raise.
        No event-loop primitives are created, including inside Workflow contexts.
        """
        subscriptions: list[tuple[Context, object]] = []
        context: Context | None = self
        while context is not None:
            subscriptions.append((context, object()))
            context = context.__parent
        claim = object()
        pending = {claim: callback}

        def cleanup() -> None:
            for context, key in subscriptions:
                context.__cancel_callbacks.pop(key, None)

        def remove() -> None:
            pending.clear()
            cleanup()

        def notify() -> None:
            # One dictionary pop claims the notification even when separate
            # threads cancel a child and its parent at the same time.
            action = pending.pop(claim, None)
            if action is not None:
                cleanup()
                action()

        for context, key in subscriptions:
            context.__cancel_callbacks[key] = notify
        if self.cancelled:
            notify()
        # Notification may have run while the loop was still installing
        # ancestor entries. Remove those late entries as well.
        if not pending:
            cleanup()
        return remove

    def child(self) -> "Context":
        """Return a cancellable child that preserves the parent's deadline."""
        return Context(_parent=self, _deadline=self.__deadline)

    @property
    def time_left(self) -> Optional[float]:
        if self.__deadline == float('inf'):
            return None
        return max(0.0, self.__deadline - time.perf_counter())

    def bounded(self, timeout: timedelta) -> "Context":
        """Return a child context capped by both this deadline and ``timeout``."""
        seconds = max(0.0, timeout.total_seconds())
        current = self.time_left
        if current is not None:
            seconds = min(seconds, current)
        return Context(
            _parent=self,
            _deadline=time.perf_counter() + seconds,
        )


def default_context() -> Context:
    return Context()

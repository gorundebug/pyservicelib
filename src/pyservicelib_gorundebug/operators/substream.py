# Copyright (c) 2026 Sergey Alexeev
# Licensed under the MIT License. See LICENSE for details.

import asyncio
from contextvars import Context, ContextVar, copy_context

from ..runtime.common import (
    Caller, RuntimeHelpers, ServiceExecutionEnvironment, Stream, StreamConsumer,
    SubStreamCollector, TypedStream, TypedSubStream,
)
from ..runtime.config.stream_types import SubStreamConfig
from ..runtime.context.request import request_cancelled, request_deadline
from ..runtime.environment.tracing import sampling_enabled, start_stream_span


async def _drain_task[T](task: asyncio.Task[T]) -> T:
    """Finish an active callback even if its waiting task is cancelled again."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class _SubStreamCall[R]:
    def __init__(self, env: ServiceExecutionEnvironment, collector: SubStreamCollector[R]) -> None:
        self.env = env
        self.loop = asyncio.get_running_loop()
        self.context: Context | None = copy_context()
        self.collector: SubStreamCollector[R] | None = collector
        self.cancelled = request_cancelled.get()
        self.deadline = request_deadline.get()
        self.done = asyncio.Event()
        self.lock = asyncio.Lock()
        self.closed = False
        self.error: BaseException | None = None

    def check_context(self) -> None:
        if self.cancelled is not None and self.cancelled.is_set():
            raise asyncio.CancelledError("request cancelled")
        if self.deadline is not None:
            now = self.env.substream_now()
            if self.deadline.tzinfo is None:
                now = now.replace(tzinfo=None)
            if now >= self.deadline:
                raise TimeoutError("substream deadline exceeded")

    async def deliver(self, value: R) -> None:
        async with self.lock:
            if self.closed:
                return
            try:
                self.check_context()
                collector, context = self.collector, self.context
                if collector is None or context is None:
                    return
                # A coroutine alone does not capture ContextVars. Restore the caller
                # context in a task so a nested collector sees the outer invocation.
                task = asyncio.create_task(collector.out(value), context=context.copy())
                try:
                    complete = await asyncio.shield(task)
                except asyncio.CancelledError:
                    await _drain_task(task)
                    raise
                if complete:
                    self.closed = True
                    self.collector = None
                    self.context = None
                    self.done.set()
            except BaseException as error:
                self.error = error
                self.closed = True
                self.collector = None
                self.context = None
                self.done.set()
                raise

    async def close(self) -> None:
        self.closed = True
        async with self.lock:
            self.collector = None
            self.context = None


class _ResultLink[T, R](StreamConsumer[R]):
    def __init__(self, entry: "SubStream[T, R]") -> None:
        self._entry = entry

    @property
    def stream(self) -> Stream:
        return self._entry

    async def consume(self, value: R) -> None:
        call = self._entry._call.get()
        if call is None or call.closed:
            return
        if asyncio.get_running_loop() is call.loop:
            await call.deliver(value)
        else:
            # A transport can deliver on another loop. Never share asyncio locks
            # between loops; serialize collector execution on the invocation loop.
            future = asyncio.run_coroutine_threadsafe(call.deliver(value), call.loop)
            await asyncio.wrap_future(future)


class SubStream[T, R](TypedSubStream[T, R]):
    def __init__(self, cfg: SubStreamConfig, env: ServiceExecutionEnvironment) -> None:
        if cfg.id_service != env.service_config.id:
            raise ValueError("SubStream must belong to its execution service")
        super().__init__(cfg.id, env, RuntimeHelpers[T](env).make_stream_serde(cfg.value_type))
        self._consumer: StreamConsumer[T] | None = None
        self._caller: Caller[T] | None = None
        self._source: TypedStream[R] | None = None
        self._call: ContextVar[_SubStreamCall[R] | None] = ContextVar(
            f"substream:{cfg.id}", default=None,
        )
        tracing = env.tracing
        self._tracer = tracing.tracer(env.service_config.name) if tracing is not None else None

    @property
    def consumer(self) -> StreamConsumer[T] | None:
        return self._consumer

    @consumer.setter
    def consumer(self, value: StreamConsumer[T]) -> None:
        if value.stream.config.id_service != self.config.id_service:
            raise ValueError("SubStream body must belong to the same service")
        self._consumer = value
        self._caller = RuntimeHelpers[T](self.environment).make_caller(self)

    @property
    def consumers(self) -> list[Stream]:
        return [self._consumer.stream] if self._consumer is not None else []

    @property
    def type_name(self) -> str:
        return self.serde.value_serializer.type_name

    def set_source(self, source: TypedStream[R]) -> None:
        if source.id == self.id or source.config.id_service != self.config.id_service:
            raise ValueError("SubStream source must be a different stream in the same service")
        if self._source is not None:
            if source is self._source:
                return
            raise ValueError("SubStream source is already configured")
        if source.consumer is not None:
            raise ValueError("SubStream source already has a consumer")
        source.consumer = _ResultLink(self)
        self._source = source

    def build(self) -> None:
        if self._caller is None or self._source is None:
            raise ValueError(f"SubStream '{self.name}' requires a body and a result source")

    async def consume(self, value: T, collector: SubStreamCollector[R]) -> None:
        self.build()
        call = _SubStreamCall(self.environment, collector)
        call.check_context()
        token = self._call.set(call)
        try:
            if self._tracer is None or not sampling_enabled():
                await self._consume(value, call)
            else:
                _, span = start_stream_span(self._tracer, "stream.substream", self)
                try:
                    with span.scoped():
                        await self._consume(value, call)
                finally:
                    span.end()
        finally:
            self._call.reset(token)
            close = asyncio.create_task(call.close())
            try:
                await asyncio.shield(close)
            except asyncio.CancelledError:
                await _drain_task(close)
                raise

    async def _consume(self, value: T, call: _SubStreamCall[R]) -> None:
        caller = self._caller
        if caller is None:
            raise ValueError("SubStream body is not configured")
        await caller.consume(value)
        await self.environment.wait_substream_result(call.done)
        if call.error is not None:
            raise call.error

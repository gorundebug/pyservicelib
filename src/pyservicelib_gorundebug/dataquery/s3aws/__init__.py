"""Async S3 Query operations using aiobotocore native request/response mappings.

The client is connector-owned. Response bodies belong to one invocation and must
be consumed by the mapper before it returns. No hidden retries or delivery queue.
"""
from collections.abc import Mapping
from inspect import isawaitable
from typing import Any, Protocol

from .. import EndpointHandler, make_endpoint_consumer
from ...runtime.common import Collect, Consumer, TypedQueryStream
from ...runtime.context.request import request_context_error


class S3Handler[HandlerState, T, R, E](EndpointHandler[HandlerState, R, E], Protocol):
    async def build_request(self, state: HandlerState, value: T) -> Mapping[str, Any]: ...
    async def handle_result(self, state: HandlerState, result: Mapping[str, Any], out: Collect[R]) -> None: ...


class GetObjectHandler[HandlerState, T, R, E](S3Handler[HandlerState, T, R, E], Protocol): pass
class HeadObjectHandler[HandlerState, T, R, E](S3Handler[HandlerState, T, R, E], Protocol): pass
class ListObjectsHandler[HandlerState, T, R, E](S3Handler[HandlerState, T, R, E], Protocol): pass
class PutObjectHandler[HandlerState, T, R, E](S3Handler[HandlerState, T, R, E], Protocol): pass
class DeleteObjectHandler[HandlerState, T, R, E](S3Handler[HandlerState, T, R, E], Protocol): pass


def _check_request() -> None:
    if (reason := request_context_error()) is not None:
        raise RuntimeError(reason)


def _make_consumer[HandlerState, T, R, E](
    stream: TypedQueryStream[T, R, E], source: Any,
    handler: S3Handler[HandlerState, T, R, E], operation: str,
) -> Consumer[T]:
    async def execute(state: HandlerState, value: T, out: Collect[R]) -> None:
        from .connector import Connector
        client = source.client if isinstance(source, Connector) else source
        if client.meta.config.retries.get('total_max_attempts') != 1:
            raise ValueError('S3 Query requires total_max_attempts=1')
        request = dict(await handler.build_request(state, value))
        if operation == 'put_object':
            # A successful builder transfers ownership of its upload body.
            body = request.get('Body')
            try:
                _check_request()
                result = await client.put_object(**request)
                _check_request()
                await handler.handle_result(state, result, out)
                _check_request()
            finally:
                close = getattr(body, 'aclose', None) or getattr(body, 'close', None)
                if close is not None:
                    completion = close()
                    if isawaitable(completion):
                        await completion
            return
        _check_request()
        if operation == 'list_objects_v2':
            # Native paginator keeps continuation tokens within this invocation.
            previous_token = request.get('ContinuationToken')
            async for page in client.get_paginator(operation).paginate(**request):
                _check_request()
                truncated = page.get('IsTruncated', False)
                next_token = page.get('NextContinuationToken')
                await handler.handle_result(state, page, out)
                if truncated:
                    if not next_token or next_token == previous_token:
                        raise ValueError('S3 listing did not advance its continuation token')
                    previous_token = next_token
            _check_request()
            return
        result = await getattr(client, operation)(**request)
        if operation == 'get_object':
            async with result['Body']:
                _check_request()
                await handler.handle_result(state, result, out)
                _check_request()
        else:
            _check_request()
            await handler.handle_result(state, result, out)
            _check_request()
    return make_endpoint_consumer(stream, handler, execute)


def make_get_object_endpoint_consumer[HandlerState, T, R, E](stream: TypedQueryStream[T, R, E], client: Any, handler: GetObjectHandler[HandlerState, T, R, E]) -> Consumer[T]:
    return _make_consumer(stream, client, handler, 'get_object')


def make_head_object_endpoint_consumer[HandlerState, T, R, E](stream: TypedQueryStream[T, R, E], client: Any, handler: HeadObjectHandler[HandlerState, T, R, E]) -> Consumer[T]:
    return _make_consumer(stream, client, handler, 'head_object')


def make_list_objects_endpoint_consumer[HandlerState, T, R, E](stream: TypedQueryStream[T, R, E], client: Any, handler: ListObjectsHandler[HandlerState, T, R, E]) -> Consumer[T]:
    return _make_consumer(stream, client, handler, 'list_objects_v2')


def make_put_object_endpoint_consumer[HandlerState, T, R, E](stream: TypedQueryStream[T, R, E], client: Any, handler: PutObjectHandler[HandlerState, T, R, E]) -> Consumer[T]:
    return _make_consumer(stream, client, handler, 'put_object')


def make_delete_object_endpoint_consumer[HandlerState, T, R, E](stream: TypedQueryStream[T, R, E], client: Any, handler: DeleteObjectHandler[HandlerState, T, R, E]) -> Consumer[T]:
    return _make_consumer(stream, client, handler, 'delete_object')

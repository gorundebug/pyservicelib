import io
import pytest
from aiohttp import web
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

from pyservicelib_gorundebug.dataquery import BeginRequest
from pyservicelib_gorundebug.dataquery import s3aws
from pyservicelib_gorundebug.dataquery.s3aws.connector import Connector
from pyservicelib_gorundebug.runtime.config.query import S3AWSDataConnectorConfig
from pyservicelib_gorundebug.runtime.context.context import Context
from .test_query_lifecycle import Stream


@pytest.mark.asyncio
async def test_native_s3_operations_and_cleanup(monkeypatch):
    requests = []
    async def serve(request):
        requests.append((request.method, request.path))
        if request.method == 'GET' and 'list-type' in request.query:
            mode = request.query.get('prefix', '')
            if mode in ('list_missing', 'list_repeat', 'list_resume'):
                token = '' if mode == 'list_missing' else '<NextContinuationToken>next</NextContinuationToken>'
                return web.Response(text=f'<ListBucketResult><IsTruncated>true</IsTruncated>{token}<Contents><Key>partial</Key></Contents></ListBucketResult>', content_type='application/xml')
            second = 'continuation-token' in request.query
            token = '' if second else '<NextContinuationToken>next</NextContinuationToken>'
            return web.Response(text=f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>{str(not second).lower()}</IsTruncated>{token}<Contents><Key>{"two" if second else "one"}</Key><Size>1</Size></Contents></ListBucketResult>', content_type='application/xml')
        if request.method == 'GET': return web.Response(body=b'payload')
        if request.method == 'PUT':
            assert await request.read() == b'upload'
            return web.Response(headers={'ETag': 'fixture'})
        if request.method == 'DELETE' and request.path.endswith('/failure'):
            return web.Response(status=503, text='<Error><Code>SlowDown</Code></Error>', content_type='application/xml')
        return web.Response(status=204 if request.method == 'DELETE' else 200)
    application = web.Application()
    application.router.add_route('*', '/{path:.*}', serve)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    address = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fixture')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fixture')
    monkeypatch.setenv('AWS_EC2_METADATA_DISABLED', 'true')
    connector = Connector(S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1', address=address, forcePathStyle=True))
    context = Context()
    try:
        with pytest.raises(RuntimeError, match='not started'): _ = connector.client
        await connector.start(context)
        client = connector.client
        await connector.start(context)
        assert connector.client is client
        for operation in ('get_object', 'head_object', 'list_objects', 'put_object', 'delete_object', 'get_failure', 'delete_failure', 'list_missing', 'list_repeat', 'list_resume'):
            before_requests = len(requests)
            stream = Stream()
            bodies = []
            upload = io.BytesIO(b'upload')
            ends = []
            class Handler:
                async def begin_request(self, _): return BeginRequest(0)
                async def build_request(self, state, value):
                    if operation.startswith('list_'):
                        return {'Bucket': 'bucket', 'Prefix': operation,
                                **({'ContinuationToken': 'next'} if operation == 'list_resume' else {})}
                    result = {'Bucket': 'bucket', 'Key': 'failure' if operation == 'delete_failure' else 'item'}
                    if operation == 'put_object': result['Body'] = upload
                    return result
                async def handle_result(self, state, result, out):
                    if 'Body' in result:
                        body = result['Body']
                        bodies.append(body)
                        if operation == 'get_failure': raise ValueError('mapper failed')
                        await out.out(await body.read(3))
                        await out.out(await body.read())
                    elif operation.startswith('list_'): await out.out(result['Contents'][0]['Key'])
                    else: await out.out('result')
                async def end_request(self, stream, state, error, out, errors):
                    assert all(body.raw_stream.closed for body in bodies)
                    if operation == 'put_object': assert upload.closed
                    ends.append(error)
            native_operation = operation.replace('get_failure', 'get_object').replace('delete_failure', 'delete_object')
            if operation.startswith('list_'): native_operation = 'list_objects'
            consumer = getattr(s3aws, f'make_{native_operation}_endpoint_consumer')(stream, connector, Handler())
            await consumer.consume(None)
            assert len(ends) == 1
            if operation in ('list_missing', 'list_repeat', 'list_resume'):
                assert isinstance(ends[0], ValueError)
                assert 'did not advance' in str(ends[0])
                expected = 2 if operation == 'list_repeat' else 1
                assert stream.values == ['partial'] * expected
                assert len(requests) - before_requests == expected
            elif operation.endswith('failure'):
                assert ends[0] is not None
            else:
                assert ends == [None]
                assert stream.values == ([b'pay', b'load'] if operation == 'get_object' else ['one', 'two'] if operation == 'list_objects' else ['result'])
        assert requests.count(('DELETE', '/bucket/failure')) == 1
    finally:
        await connector.stop(context)
        await runner.cleanup()
    with pytest.raises(RuntimeError, match='not started'): _ = connector.client


def test_s3_configuration_and_projection():
    from pydantic import ValidationError
    from pyservicelib_gorundebug.runtime.config.config_to_api import data_connector_config_to_api
    cfg = S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1', address='http://private-store:9000', forcePathStyle=True)
    projected = data_connector_config_to_api(cfg)
    assert projected.region == 'us-east-1'
    assert projected.force_path_style is True
    assert projected.address is None
    assert projected.implementations is None
    assert 'private-store' not in repr(cfg)
    for address in ('http://user:secret@store', 'file:///secret', 'https://store?token=secret'):
        with pytest.raises(ValidationError) as exc:
            S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1', address=address)
        assert 'secret' not in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['body_cancel', 'body_deadline', 'page_cancel', 'upload_cancel', 'service_stop'])
async def test_native_s3_interruption_closes_resources_before_end(monkeypatch, mode):
    import asyncio
    from datetime import timedelta
    from pyservicelib_gorundebug.runtime.context.request import request_context_scope

    requests, bodies, outcomes, order = [], [], [], []
    blocked = asyncio.Event()
    release_server = asyncio.Event()
    mapped_first = asyncio.Event()
    upload = io.BytesIO(b'upload')

    async def serve(request):
        requests.append((request.method, request.path))
        if 'list-type' in request.query:
            if 'continuation-token' not in request.query:
                return web.Response(text='<ListBucketResult><IsTruncated>true</IsTruncated>'
                    '<NextContinuationToken>next</NextContinuationToken>'
                    '<Contents><Key>one</Key><Size>1</Size></Contents></ListBucketResult>',
                    content_type='application/xml')
            blocked.set()
            await release_server.wait()
            return web.Response(text='<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>',
                content_type='application/xml')
        if request.method == 'PUT':
            assert await request.read() == b'upload'
            blocked.set()
            await release_server.wait()
            return web.Response()
        response = web.StreamResponse(headers={'Content-Length': '100'})
        await response.prepare(request)
        await response.write(b'first')
        blocked.set()
        await release_server.wait()
        if mode == 'service_stop':
            await response.write(b'x' * 95)
            await response.write_eof()
        return response

    app = web.Application()
    app.router.add_route('*', '/{path:.*}', serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    address = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    for name, value in [('AWS_ACCESS_KEY_ID', 'fixture'), ('AWS_SECRET_ACCESS_KEY', 'fixture'),
                        ('AWS_EC2_METADATA_DISABLED', 'true')]:
        monkeypatch.setenv(name, value)
    connector = Connector(S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1',
        address=address, forcePathStyle=True))
    context = Context()
    task = stopped = None
    try:
        await connector.start(context)
        stream = Stream()
        class Handler:
            async def begin_request(self, _): return BeginRequest('invocation')
            async def build_request(self, state, value):
                request = {'Bucket': 'bucket'}
                if mode != 'page_cancel': request['Key'] = 'object'
                if mode == 'upload_cancel': request['Body'] = upload
                return request
            async def handle_result(self, state, result, out):
                if mode == 'page_cancel':
                    await out.out(result['Contents'][0]['Key'])
                    mapped_first.set()
                    return
                body = result['Body']
                bodies.append(body)
                await out.out(await body.read(5))
                mapped_first.set()
                await body.read()  # Deliberately incomplete native HTTP body.
            async def end_request(self, _, state, error, out, errors):
                assert state == 'invocation'
                assert all(body.raw_stream.closed for body in bodies)
                if mode == 'upload_cancel': assert upload.closed
                order.append('end')
                outcomes.append(error)
                await out.out('finished')
        operation = 'list_objects' if mode == 'page_cancel' else 'put_object' if mode == 'upload_cancel' else 'get_object'
        consumer = getattr(s3aws, f'make_{operation}_endpoint_consumer')(stream, connector, Handler())
        request_context = Context(timeout=timedelta(seconds=1)) if mode == 'body_deadline' else Context()
        async def invoke():
            with request_context_scope(request_context):
                await consumer.consume(None)
        async with asyncio.timeout(5):
            task = asyncio.create_task(invoke())
            await blocked.wait()
            if mode != 'upload_cancel': await mapped_first.wait()
            if mode == 'service_stop':
                from pyservicelib_gorundebug.runtime.serviceapp import ServiceApp
                from .test_serviceapp_shutdown import RecordingLogger
                pools_closed = asyncio.Event()
                class TestApp(ServiceApp):
                    @property
                    def service_config(self):
                        return type('Config', (), {'shutdown_timeout': 3000})()
                class Loader:
                    async def stop(self): pass
                class Pool:
                    async def stop(self, ctx): pools_closed.set()
                class Engine:
                    async def shutdown(self): pass
                service = TestApp()
                service._loader = Loader()
                service._log = RecordingLogger()
                service._delay_pool = Pool()
                service._metrics_engine = service._logs_engine = Engine()
                service.add_managed_data_connector(connector)
                service._tasks.add(task)
                task.add_done_callback(service._tasks.discard)
                stopped = asyncio.create_task(service.stop(Context(timedelta(seconds=3))))
                await pools_closed.wait()
                assert not stopped.done()
                assert outcomes == []
                assert connector.client is not None
                release_server.set()
                await stopped
                await task
                assert outcomes == [None]
                with pytest.raises(RuntimeError, match='not started'): _ = connector.client
                assert not service._tasks
                assert not any(level in ('warn', 'error') for level, _, _ in service._log.records)
            elif mode == 'body_deadline':
                await task
                assert isinstance(outcomes[0], TimeoutError)
            else:
                request_context.cancel()
                with pytest.raises(asyncio.CancelledError): await task
                assert isinstance(outcomes[0], asyncio.CancelledError)
        assert len(outcomes) == 1
        assert stream.values == (['finished'] if mode == 'upload_cancel' else
            ['one', 'finished'] if mode == 'page_cancel' else [b'first', 'finished'])
        assert len(requests) == (2 if mode == 'page_cancel' else 1)
        if mode != 'service_stop':
            assert connector.client is not None  # Only this invocation was cancelled.
        await connector.stop(context)
        order.append('connector_closed')
        assert order == ['end', 'connector_closed']
    finally:
        release_server.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if stopped is not None and not stopped.done():
            await stopped
        await connector.stop(context)
        await runner.cleanup()

@pytest.mark.asyncio
async def test_empty_missing_and_forbidden_remain_distinct(monkeypatch):
    requests = []

    async def serve(request):
        requests.append((request.method, request.path))
        if 'list-type' in request.query:
            return web.Response(text='<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>', content_type='application/xml')
        key = request.path.rsplit('/', 1)[-1]
        if key in ('missing', 'forbidden'):
            return web.Response(status=404 if key == 'missing' else 403,
                                text=f'<Error><Code>{"NoSuchKey" if key == "missing" else "AccessDenied"}</Code></Error>',
                                content_type='application/xml')
        return web.Response(body=b'', headers={'Content-Length': '0'})

    app = web.Application()
    app.router.add_route('*', '/{path:.*}', serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fixture')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fixture')
    monkeypatch.setenv('AWS_EC2_METADATA_DISABLED', 'true')
    address = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    connector = Connector(S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1', address=address, forcePathStyle=True))
    context = Context()
    try:
        await connector.start(context)
        for operation in ('get_object', 'head_object', 'list_objects'):
            for key in (('empty',) if operation == 'list_objects' else ('empty', 'missing', 'forbidden')):
                stream, ends, bodies = Stream(), [], []
                before = len(requests)

                class Handler:
                    async def begin_request(self, _): return BeginRequest(key)
                    async def build_request(self, state, value):
                        return {'Bucket': 'bucket', **({'Prefix': 'absent/'} if operation == 'list_objects' else {'Key': state})}
                    async def handle_result(self, state, result, out):
                        assert state == 'empty'
                        if operation == 'get_object':
                            bodies.append(result['Body'])
                            assert await result['Body'].read() == b''
                        elif operation == 'head_object': assert result['ContentLength'] == 0
                        else: assert result.get('Contents', []) == []
                        await out.out('empty-success')
                    async def end_request(self, _, state, error, out, errors):
                        ends.append(error)
                        assert all(body.raw_stream.closed for body in bodies)
                        if state == 'empty':
                            assert error is None
                            await out.out('completed')
                        else:
                            assert error.response['ResponseMetadata']['HTTPStatusCode'] == (404 if state == 'missing' else 403)

                consumer = getattr(s3aws, f'make_{operation}_endpoint_consumer')(stream, connector, Handler())
                await consumer.consume(None)
                assert len(ends) == 1
                assert len(requests) - before == 1
                assert stream.values == (['empty-success', 'completed'] if key == 'empty' else [])
    finally:
        await connector.stop(context)
        await runner.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['success', 'mapper_error', 'cancel'])
async def test_large_native_body_streaming_and_cleanup(monkeypatch, mode):
    import asyncio

    size = 16 * 1024 * 1024
    release = asyncio.Event()
    calls = 0
    async def serve(request):
        nonlocal calls
        calls += 1
        response = web.StreamResponse(headers={'Content-Length': str(size)})
        await response.prepare(request)
        try:
            await response.write(b'x' * 4096)
            # Native Get must reach the mapper before receiving the whole body.
            await asyncio.wait_for(release.wait(), 5)
            chunk = b'x' * (64 * 1024)
            remaining = size - 4096
            while remaining:
                n = min(remaining, len(chunk))
                await response.write(chunk[:n])
                remaining -= n
            await response.write_eof()
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return response

    application = web.Application()
    application.router.add_get('/{path:.*}', serve)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fixture')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fixture')
    monkeypatch.setenv('AWS_EC2_METADATA_DISABLED', 'true')
    address = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    connector = Connector(S3AWSDataConnectorConfig(id=1, name='objects', region='us-east-1', address=address, forcePathStyle=True))
    context = Context()
    stream = Stream()
    bodies, ends = [], []
    received = 0
    class Handler:
        async def begin_request(self, _): return BeginRequest(0)
        async def build_request(self, state, value): return {'Bucket': 'bucket', 'Key': 'large'}
        async def handle_result(self, state, result, out):
            nonlocal received
            body = result['Body']
            bodies.append(body)
            first = await body.read(4096)
            assert first == b'x' * 4096
            received += len(first)
            release.set()
            if mode == 'mapper_error': raise ValueError('mapper stopped')
            if mode == 'cancel':
                asyncio.current_task().cancel()
                await asyncio.sleep(0)
            while chunk := await body.read(32 * 1024):
                assert chunk == b'x' * len(chunk)
                received += len(chunk)
            await out.out(received)
        async def end_request(self, stream, state, error, out, errors):
            assert len(bodies) == 1 and bodies[0].raw_stream.closed
            ends.append(error)
    try:
        await connector.start(context)
        consumer = s3aws.make_get_object_endpoint_consumer(stream, connector, Handler())
        if mode == 'cancel':
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer.consume(None), 10)
        else:
            await asyncio.wait_for(consumer.consume(None), 10)
        assert calls == 1 and len(ends) == 1
        if mode == 'success':
            assert ends == [None] and stream.values == [size] and received == size
        else:
            assert isinstance(ends[0], ValueError if mode == 'mapper_error' else asyncio.CancelledError)
            assert stream.values == []
    finally:
        release.set()
        await connector.stop(context)
        await runner.cleanup()

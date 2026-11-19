"""TLS through the default native client; no verification bypass or custom maker."""
import ssl
from pathlib import Path

import pytest
from aiohttp import web
from botocore.exceptions import SSLError
from pyservicelib_gorundebug.dataquery.s3aws.connector import Connector
from pyservicelib_gorundebug.runtime.config.query import S3AWSDataConnectorConfig
from pyservicelib_gorundebug.runtime.context.context import Context

@pytest.mark.asyncio
async def test_s3_native_ca_bundle(monkeypatch, tmp_path):
    fixture = Path(__file__).parent / 'fixtures/query-tls'
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(fixture / 'ca.pem', fixture / 'key.pem')
    app = web.Application()
    async def head(request):
        assert request.secure
        return web.Response()
    app.router.add_head('/bucket/key', head)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0, ssl_context=tls)
    await site.start()
    for key, value in {'AWS_ACCESS_KEY_ID':'fixture', 'AWS_SECRET_ACCESS_KEY':'fixture',
        'AWS_SESSION_TOKEN':'', 'AWS_EC2_METADATA_DISABLED':'true',
        'AWS_CONFIG_FILE':str(tmp_path/'config'), 'AWS_SHARED_CREDENTIALS_FILE':str(tmp_path/'credentials')}.items():
        monkeypatch.setenv(key,value)
    monkeypatch.delenv('AWS_PROFILE', raising=False)
    monkeypatch.delenv('AWS_DEFAULT_PROFILE', raising=False)
    context = Context()
    try:
        for trusted in (False, True):
            monkeypatch.delenv('AWS_CA_BUNDLE', raising=False)
            if trusted: monkeypatch.setenv('AWS_CA_BUNDLE', str(fixture/'ca.pem'))
            connector = Connector(S3AWSDataConnectorConfig(id=1, name='tls', region='us-east-1',
                address=f'https://127.0.0.1:{site._server.sockets[0].getsockname()[1]}', forcePathStyle=True))
            await connector.start(context)
            try:
                if trusted:
                    assert (await connector.client.head_object(Bucket='bucket',Key='key'))['ResponseMetadata']['HTTPStatusCode'] == 200
                else:
                    with pytest.raises(SSLError):
                        await connector.client.head_object(Bucket='bucket',Key='key')
            finally: await connector.stop(context)
    finally: await runner.cleanup()

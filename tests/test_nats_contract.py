import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pyservicelib_gorundebug.runtime.config import (
    NatsDataConnectorConfig, JetStreamDataConnectorConfig, NatsEndpointConfig,
    JetStreamEndpointConfig, data_connector_config_to_api, endpoint_config_to_api,
)
from pyservicelib_gorundebug.runtime.nats import connection_options, validate_endpoint, covers, wait_context, TransportStopped
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.datasource.nats import ConsumerMessage, JetStreamMessage, ResultContext


def connector(**kwargs):
    return NatsDataConnectorConfig(id=1, name='nats', servers=['nats://127.0.0.1:4222'], **kwargs)


def test_configuration_roundtrip_and_native_units():
    cfg = connector(connectTimeoutMs=1250, operationTimeoutMs=2500, token='runtime')
    options = connection_options(cfg)
    assert options['connect_timeout'] == 1.25
    assert options['flush_timeout'] == 2.5
    assert options['allow_reconnect'] is True
    assert options['pending_size'] > 0
    api = data_connector_config_to_api(cfg)
    assert api.implementations == {'python': 'nats.py'}
    assert api.servers == cfg.servers and api.token == 'runtime'
    ep = JetStreamEndpointConfig(id=2, name='events', idDataConnector=1, functionName='Handler', subject='events', streamName='S', durableName='D', createConsumer=False)
    output = endpoint_config_to_api(ep)
    assert output.subject == 'events' and output.durable_name == 'D'
    assert output.create_consumer is False


@pytest.mark.parametrize('change', [
    {'servers': []}, {'servers': ['localhost:4222']}, {'servers': ['https://localhost']},
    {'servers': ['nats://user:secret@localhost']}, {'servers': ['nats://localhost/']},
    {'servers': ['nats://localhost?q=x']}, {'servers': ['nats://localhost#x']},
    {'servers': ['nats://localhost:0']}, {'servers': ['nats://localhost:65536']},
    {'servers': ['tls://localhost']}, {'servers': ['nats://localhost', 'tls://localhost']},
    {'username': 'u'}, {'password': 'p'}, {'token': 't', 'credentials_file': 'a'},
    {'username': 'u', 'password': 'p', 'token': 't'}, {'tls_ca_file': 'a'},
    {'connect_timeout_ms': 0}, {'operation_timeout_ms': 9223372036855},
    {'connect_timeout_ms': True},
])
def test_runtime_rejects_invalid_configuration(change):
    values = connector().model_dump()
    values.update(change)
    with pytest.raises(ValueError):
        connection_options(SimpleNamespace(**values))


@pytest.mark.parametrize('pattern,subject,expected', [('events.>', 'events.*', True), ('events.*', 'events.>', False), ('events.*','events.x',True), ('events','events.*',False),('>','events',True)])
def test_stream_subject_coverage(pattern, subject, expected):
    assert covers(pattern, subject) == expected


def test_core_has_no_fake_settlement_and_endpoint_rules():
    msg = ConsumerMessage(SimpleNamespace(data=b'\x00\xff',subject='a',reply='',headers={}))
    assert msg.data == b'\x00\xff' and not hasattr(msg, 'ack')
    cfg = NatsEndpointConfig(id=1,name='a',idDataConnector=1,functionName='Handler',subject='a.*')
    validate_endpoint(cfg, jetstream=False, source=True)
    with pytest.raises(ValueError):
        validate_endpoint(cfg, jetstream=False, source=False)
    cfg.create_stream = True
    with pytest.raises(ValueError):
        validate_endpoint(cfg, jetstream=False, source=True)


@pytest.mark.asyncio
async def test_local_done_does_not_ack_and_old_handles_fail():
    native = SimpleNamespace(data=b'a',subject='a',reply='reply',headers={},metadata='native', ack=AsyncMock())
    connection = SimpleNamespace(epoch=0,closed=False,nc=SimpleNamespace(is_connected=True))
    msg = JetStreamMessage(native,connection)
    result = ResultContext(None, Context())
    result.done(); result.done()
    native.ack.assert_not_awaited()
    connection.epoch += 1
    with pytest.raises(TransportStopped):
        await msg.ack()
    native.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_wait_cancelled_but_admitted_future_retained():
    owner = Context(timeout=timedelta(milliseconds=5))
    future = asyncio.get_running_loop().create_future()
    with pytest.raises(TimeoutError):
        await wait_context(future, owner, owned=False)
    assert not future.cancelled()
    future.set_result('late')
    assert await future == 'late'


def test_trace_carrier_case_insensitive_and_caller_headers_preserved():
    from pyservicelib_gorundebug.runtime.nats import incoming_headers, outgoing_headers
    from pyservicelib_gorundebug.runtime.context.request import request_context_scope, with_stream_id
    trace='00-12345678901234567890123456789012-1234567890123456-01'
    with request_context_scope(Context()):
        with incoming_headers({'TraceParent':trace,'TraceState':'vendor=opaque','Baggage':'tenant=blue','X-Trace':'1'}):
            with_stream_id('inherited')
            headers=outgoing_headers({'Business':'untouched','X-Stream-ID':'stale','TRACEPARENT':'stale'})
    assert headers['Business']=='untouched' and headers['x-stream-id']=='inherited'
    assert headers['traceparent']==trace and headers['tracestate']=='vendor=opaque'
    assert headers['baggage']=='tenant=blue' and headers['x-trace']=='1'
    assert 'TRACEPARENT' not in headers and 'X-Stream-ID' not in headers


def test_malformed_credentials_rejected_before_official_parser(tmp_path):
    path=tmp_path/'invalid.creds'; path.write_text('no JWT or seed markers')
    with pytest.raises(ValueError,match='Malformed'):
        connection_options(connector(credentialsFile=str(path)))


@pytest.mark.parametrize('kind,connector_class,endpoint_class',[
    (7,NatsDataConnectorConfig,NatsEndpointConfig),
    (8,JetStreamDataConnectorConfig,JetStreamEndpointConfig),
])
def test_generated_named_adapters_preserve_runtime_fields_and_properties(kind,connector_class,endpoint_class):
    from pyservicelib_gorundebug.runtime.config.config import DataConnectorConfig,EndpointConfig
    raw=DataConnectorConfig.from_dict({'id':1,'name':'broker','type':kind,'implementation':'nats.py',
        'servers':['nats://localhost:4222'],'username':'runtime-user','password':'runtime-pass',
        'tlsEnabled':True,'tlsServerName':'broker.internal','tlsCaFile':'/ca.pem',
        'tlsCertFile':'/cert.pem','tlsKeyFile':'/key.pem','customSetting':42})
    cfg=connector_class.model_validate({**raw.model_dump(exclude_none=True),'properties':raw.properties})
    assert cfg.type.value==kind and cfg.implementation=='nats.py'
    assert cfg.username=='runtime-user' and cfg.password=='runtime-pass'
    assert cfg.tls_enabled and cfg.tls_server_name=='broker.internal'
    assert cfg.tls_ca_file=='/ca.pem' and cfg.tls_cert_file=='/cert.pem' and cfg.tls_key_file=='/key.pem'
    assert cfg.custom_setting==42
    endpoint=EndpointConfig.from_dict({'id':2,'name':'receive','idDataConnector':1,'functionName':'Handle',
        'subject':'events','streamName':'S','durableName':'D','enabled':True,'createConsumer':False,'customEndpoint':7})
    ep=endpoint_class.model_validate({**endpoint.model_dump(exclude_none=True),'properties':endpoint.properties})
    assert ep.id_data_connector==1 and ep.enabled and ep.subject=='events'
    assert ep.function_name=='Handle' and ep.custom_endpoint==7 and ep.create_consumer is False


def test_generated_implementation_enum_is_canonical():
    from pyservicelib_gorundebug.api.models.data_connector_implementation import DataConnectorImplementation
    from pyservicelib_gorundebug.runtime.config.config import DataConnectorConfig
    assert DataConnectorImplementation('nats.py') is DataConnectorImplementation.NatsPy
    raw=DataConnectorConfig(id=1,name='broker',type=7,servers=['nats://localhost:4222'],
                            implementation=DataConnectorImplementation.NatsPy)
    assert NatsDataConnectorConfig.model_validate(raw.model_dump(exclude_none=True)).implementation=='nats.py'


async def test_connection_cleanup_before_connect_is_safe():
    from pyservicelib_gorundebug.runtime.nats import Connection
    connection = Connection(connector(), AsyncMock())
    await connection.close(Context())
    await connection.close(Context())
    assert connection.closed
    assert not connection.ready.is_set()

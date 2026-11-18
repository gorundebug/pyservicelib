"""Real native broker acceptance; set NATS_SERVER_BINARY to an official server."""
import asyncio
import os
import shutil
import socket
import subprocess
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import nats
import pytest
import pytest_asyncio
from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy, ReplayPolicy
from nats.js.errors import NotFoundError
from pyservicelib_gorundebug.runtime.config import NatsDataConnectorConfig, JetStreamDataConnectorConfig, NatsEndpointConfig, JetStreamEndpointConfig
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.context.request import stream_id_from_context, with_stream_id
from pyservicelib_gorundebug.runtime.environment.metrics.metrics import NoopMetrics
from pyservicelib_gorundebug.runtime.nats import AdmissionRejected, TransportStopped
from pyservicelib_gorundebug.datasource.nats import make_nats_endpoint_consumer as source_factory
from pyservicelib_gorundebug.datasink.nats import make_nats_endpoint_consumer as sink_factory, make_nats_endpoint_consumer_with_result as sink_result_factory


@pytest_asyncio.fixture(loop_scope="function")
async def broker(tmp_path):
    binary = os.environ.get('NATS_SERVER_BINARY') or shutil.which('nats-server')
    if not binary: pytest.skip('Real broker requires NATS_SERVER_BINARY')
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    log = (tmp_path / 'server.log').open('w')
    command = [binary, '-js', '-a', '127.0.0.1', '-p', str(port), '-sd', str(tmp_path / 'data')]
    proc = subprocess.Popen(command, stdout=log, stderr=log)
    url = f'nats://127.0.0.1:{port}'
    for _ in range(100):
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close(); await writer.wait_closed(); break
        except OSError: await asyncio.sleep(.02)
    else:
        proc.terminate(); proc.wait(timeout=5); pytest.fail('NATS server did not start')
    admin = await nats.connect(url)
    state = SimpleNamespace(url=url, admin=admin, js=admin.jetstream(), proc=proc, command=command, log=log)
    try: yield state
    finally:
        await admin.close(); state.proc.terminate(); state.proc.wait(timeout=5); log.close()


def setup(url, *, js=False, result=False, **endpoint_options):
    cfg = (JetStreamDataConnectorConfig if js else NatsDataConnectorConfig)(id=1,name='broker',servers=[url],operationTimeoutMs=800)
    values = dict(id=2,name='events',idDataConnector=1,subject='events',enabled=True,functionName='Handle')
    if js: values.update(streamName='S')
    values.update(endpoint_options)
    ep = (JetStreamEndpointConfig if js else NatsEndpointConfig)(**values)
    sources, sinks = {}, {}
    env = SimpleNamespace(config=SimpleNamespace(get_data_connector_by_id=lambda _: cfg, get_endpoint_config_by_id=lambda _: ep),metrics=NoopMetrics(),log=Mock(),tracing=None)
    env.get_datasource, env.get_datasink = sources.get, sinks.get
    env.add_datasource = lambda ds: sources.__setitem__(ds.id,ds)
    env.add_datasink = lambda ds: sinks.__setitem__(ds.id,ds)
    received,results,errors=[],[],[]
    async def collect(value): received.append((value,stream_id_from_context()))
    async def out(value): results.append((value,stream_id_from_context()))
    async def error(value): errors.append((value,stream_id_from_context()))
    stream=SimpleNamespace(environment=env,endpoint_id=2,name='flow',consume=collect,consume_result=out,
        error_stream=SimpleNamespace(consume=error),get_result_stream=lambda: object() if result else None,
        set_sink_consumer=lambda c: setattr(stream,'sink_consumer',c),set_result_consumer=lambda c: setattr(stream,'result_consumer',c))
    return SimpleNamespace(env=env,stream=stream,cfg=cfg,ep=ep,sources=sources,sinks=sinks,received=received,results=results,errors=errors)


class SourceHandler:
    def __init__(self,mode='done'):
        self.mode,self.messages,self.ended,self.errors=mode,[],[],[]
        self.entered,self.finished,self.release=asyncio.Event(),asyncio.Event(),asyncio.Event()
        self.result=None
    def concurrency(self,sc): return 1
    async def on_error(self,sc,error): self.errors.append(error)
    async def begin_request(self,sc):
        if self.mode=='begin_error': raise ValueError('begin failed')
        return 'state'
    async def consume_message(self,sc,state,msg,result):
        self.messages.append(msg); self.result=result; self.entered.set()
        if self.mode=='consume_error': raise ValueError('consume failed')
        if self.mode=='async_error': result.cancel(ValueError('async failed')); return
        if self.mode=='wait': await self.release.wait()
        if self.mode=='callback':
            async def callback(sc,state,value):
                await msg.ack_sync(); result.done(); return True
            result.set_result_callback('key',callback)
            await sc.collect(msg.data); return
        if self.mode in ('ack','wait'): await msg.ack_sync()
        await sc.collect(msg.data); result.done()
    def get_message_id(self,sc,state,value): return 'key'
    async def end_request(self,sc,error,state): self.ended.append(error); self.finished.set()


class SinkHandler:
    def __init__(self,sync=False): self.sync,self.ended=sync,[]
    def get_stream_id(self,value): return 'unused'
    def begin_request(self,stream): return 'state'
    async def consume_message(self,stream,state,value,msg):
        msg.data=value; msg.headers={'Correlation':'native'}
        if self.sync: await msg.out(await msg.send_sync())
        else: msg.send(lambda outcome: outcome)
    async def end_request(self,stream,error,state): self.ended.append(error)


async def eventually(predicate):
    async with asyncio.timeout(4):
        while not predicate(): await asyncio.sleep(.01)


async def stop(ds,seconds=2):
    await asyncio.wait_for(ds.stop(Context(timeout=timedelta(seconds=seconds))),3)


@pytest.mark.asyncio
async def test_core_native_bytes_and_no_ack(broker):
    f=setup(broker.url); handler=SourceHandler(); source_factory(f.stream,handler)
    ds=f.sources[1]; await ds.start(Context())
    try:
        await broker.admin.publish('events',b'\x00\xffhello',headers={'Correlation':'source'}); await broker.admin.flush()
        await eventually(lambda: handler.ended)
        assert f.received[0][0]==b'\x00\xffhello' and f.received[0][1]
        assert handler.messages[0].headers['Correlation']=='source'
        assert not hasattr(handler.messages[0],'ack') and handler.ended==[None]
    finally: await stop(ds)


@pytest.mark.asyncio
@pytest.mark.parametrize('sync',[False,True])
@pytest.mark.parametrize('js',[False,True])
@pytest.mark.parametrize('result',[False,True])
async def test_native_sink_results_and_correlation(broker,sync,js,result):
    f=setup(broker.url,js=js,createStream=js); handler=SinkHandler(sync)
    (sink_result_factory if result else sink_factory)(f.stream,handler)
    ds=f.sinks[1]; sub=await broker.admin.subscribe('events'); await ds.start(Context())
    try:
        with_stream_id('original-correlation'); await f.stream.sink_consumer.consume(b'\x00\xffnative')
        target=f.results if result else f.errors; await eventually(lambda: target)
        outcome,correlation=target[0]
        assert correlation=='original-correlation' and outcome.error is None
        assert outcome.publication.kind==('jetstream' if js else 'flushed' if sync else 'accepted')
        if js: assert outcome.publication.ack.stream=='S' and outcome.publication.ack.seq==1
        msg=await sub.next_msg(timeout=1)
        assert msg.data==b'\x00\xffnative' and msg.headers['Correlation']=='native'
        assert handler.ended==[None]
    finally: await stop(ds)


@pytest.mark.asyncio
async def test_js_done_not_ack_redelivers_and_explicit_ack(broker):
    await broker.js.add_stream(name='S',subjects=['events'])
    await broker.js.add_consumer('S',ConsumerConfig(durable_name='D',filter_subject='events',ack_policy=AckPolicy.EXPLICIT,deliver_policy=DeliverPolicy.ALL,replay_policy=ReplayPolicy.INSTANT,ack_wait=.15))
    await broker.js.publish('events',b'hello')
    f=setup(broker.url,js=True,durableName='D',result=True); handler=SourceHandler(); source_factory(f.stream,handler)
    ds=f.sources[1]; await ds.start(Context())
    try:
        await eventually(lambda: len(handler.messages)>=2)
        assert handler.messages[1].metadata.num_delivered>=2
        assert (await broker.js.consumer_info('S','D')).num_ack_pending==1
        with pytest.raises(TransportStopped): await handler.messages[0].ack()
        handler.mode='ack'; await eventually(lambda: len(handler.ended)>=3)
        assert (await broker.js.consumer_info('S','D')).num_ack_pending==0
    finally: await stop(ds)


@pytest.mark.asyncio
async def test_resource_creation_explicit_and_existing_unchanged(broker):
    f=setup(broker.url,js=True,durableName='D'); source_factory(f.stream,SourceHandler())
    with pytest.raises(ValueError,match='createStream=false'): await f.sources[1].start(Context())
    with pytest.raises(NotFoundError): await broker.js.stream_info('S')
    f=setup(broker.url,js=True,durableName='D',createStream=True,createConsumer=True); source_factory(f.stream,SourceHandler())
    await f.sources[1].start(Context()); await stop(f.sources[1])
    info=await broker.js.consumer_info('S','D'); before=info.config.as_dict()
    assert info.config.ack_policy==AckPolicy.EXPLICIT
    stream=await broker.js.stream_info('S')
    assert stream.config.storage=='file' and stream.config.num_replicas==1
    f=setup(broker.url,js=True,durableName='D',subject='events.other',createStream=True,createConsumer=True); source_factory(f.stream,SourceHandler())
    with pytest.raises(ValueError,match='left unchanged'): await f.sources[1].start(Context())
    assert (await broker.js.consumer_info('S','D')).config.as_dict()==before


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['begin_error','consume_error','async_error'])
async def test_source_errors_never_ack(broker,mode):
    f=setup(broker.url,js=True,durableName='D',createStream=True,createConsumer=True,result=True)
    handler=SourceHandler(mode); source_factory(f.stream,handler); ds=f.sources[1]; await ds.start(Context())
    try:
        await broker.js.publish('events',b'work')
        await eventually(lambda: handler.errors if mode=='begin_error' else handler.ended)
        assert (await broker.js.consumer_info('S','D')).num_ack_pending==1
        if mode=='begin_error': assert handler.ended==[]
        else: assert str(handler.ended[0])==('consume failed' if mode=='consume_error' else 'async failed')
        assert not ds.get_endpoint(2).consumer.pending
    finally: await stop(ds)


@pytest.mark.asyncio
async def test_source_stop_drains_active_request_before_close(broker):
    f=setup(broker.url,js=True,durableName='D',createStream=True,createConsumer=True,result=True)
    handler=SourceHandler('wait'); source_factory(f.stream,handler); ds=f.sources[1]; await ds.start(Context())
    await broker.js.publish('events',b'work'); await asyncio.wait_for(handler.entered.wait(),1)
    stopping=asyncio.create_task(stop(ds)); await asyncio.sleep(.03)
    assert not stopping.done() and ds.connection.nc.is_connected
    handler.release.set(); await stopping
    assert handler.ended==[None] and (await broker.js.consumer_info('S','D')).num_ack_pending==0


@pytest.mark.asyncio
async def test_source_stop_deadline_no_ack(broker):
    f=setup(broker.url,js=True,durableName='D',createStream=True,createConsumer=True,result=True)
    handler=SourceHandler('wait'); source_factory(f.stream,handler); ds=f.sources[1]; await ds.start(Context())
    await broker.js.publish('events',b'work'); await asyncio.wait_for(handler.entered.wait(),1)
    await stop(ds,.03)
    assert handler.ended and isinstance(handler.ended[0],TransportStopped)
    assert (await broker.js.consumer_info('S','D')).num_ack_pending==1
    assert not ds.get_endpoint(2).consumer.pending and ds.connection.nc.is_closed


@pytest.mark.asyncio
async def test_callback_completion_late_result_and_inherited_headers(broker):
    f=setup(broker.url,js=True,result=True,durableName='D',createStream=True,createConsumer=True)
    handler=SourceHandler('callback'); source_factory(f.stream,handler)
    ds=f.sources[1]; await ds.start(Context())
    try:
        await broker.js.publish('events',b'work',headers={'X-Stream-ID':'inherited', 'Baggage':'tenant=blue',
            'TraceParent':'00-12345678901234567890123456789012-1234567890123456-01','TraceState':'vendor=opaque','X-Trace':'1'})
        await eventually(lambda: f.received)
        assert f.received[0][1]=='inherited' and handler.ended==[]
        with_stream_id('inherited'); await f.stream.result_consumer.consume('result')
        await eventually(lambda: handler.ended)
        assert (await broker.js.consumer_info('S','D')).num_ack_pending==0
        await f.stream.result_consumer.consume('late')
        assert handler.ended==[None] and not ds.get_endpoint(2).consumer.pending
    finally: await stop(ds)


@pytest.mark.asyncio
@pytest.mark.parametrize('js',[False,True])
async def test_reconnect_resumes_official_source_and_sink(broker,js):
    source=setup(broker.url,js=js,**({'durableName':'D','createStream':True,'createConsumer':True} if js else {}))
    handler=SourceHandler('ack' if js else 'done'); source_factory(source.stream,handler)
    ds=source.sources[1]; await ds.start(Context())
    target=setup(broker.url,js=js); sink_result_factory(target.stream,SinkHandler())
    sink=target.sinks[1]; await sink.start(Context())
    try:
        await target.stream.sink_consumer.consume(b'before')
        await eventually(lambda: handler.ended)
        broker.proc.terminate(); broker.proc.wait(timeout=5)
        await eventually(lambda: ds.connection.epoch>0 and sink.connection.epoch>0)
        # Retired delivery cannot settle any replacement delivery after reconnect.
        if js:
            with pytest.raises(TransportStopped): await handler.messages[0].ack()
        broker.proc=subprocess.Popen(broker.command,stdout=broker.log,stderr=broker.log)
        await eventually(lambda: ds.connection.ready.is_set() and sink.connection.ready.is_set())
        await target.stream.sink_consumer.consume(b'after')
        await eventually(lambda: len(handler.ended)>=2)
        assert [v for v,_ in source.received]==[b'before',b'after']
    finally:
        await stop(sink); await stop(ds)


@pytest.mark.asyncio
async def test_sink_bounded_admission_delayed_results_and_drain(broker):
    f=setup(broker.url,js=True,createStream=True); handler=SinkHandler()
    sink_result_factory(f.stream,handler); ds=f.sinks[1]; await ds.start(Context())
    native=ds.connection.js.publish_async
    release=asyncio.Event(); entered=asyncio.Event()
    async def delayed(*args,**kwargs):
        entered.set(); await release.wait(); return await native(*args,**kwargs)
    ds.connection.js.publish_async=delayed
    try:
        await f.stream.sink_consumer.consume(b'first'); await entered.wait()
        # Four active publishers + 64 queued; no task spawned for a rejected send.
        for _ in range(100): await f.stream.sink_consumer.consume(b'queued')
        assert ds.queue.qsize()==64 and len(ds.workers)==4
        assert any(isinstance(e,AdmissionRejected) for e in handler.ended)
        assert not f.results
        stopping=asyncio.create_task(stop(ds)); await asyncio.sleep(.02)
        assert not stopping.done()
        release.set(); await stopping
        assert f.results and all(outcome.error is None for outcome,_ in f.results)
        assert ds.queue.empty() and all(w.done() for w in ds.workers)
    finally:
        release.set(); await stop(ds)


@pytest.mark.asyncio
async def test_sink_rejection_and_deadline_are_observable(broker):
    f=setup(broker.url,js=True,createStream=True); handler=SinkHandler()
    sink_result_factory(f.stream,handler); ds=f.sinks[1]; await ds.start(Context())
    gate=asyncio.Event()
    async def blocked(*args,**kwargs): await gate.wait()
    ds.connection.js.publish_async=blocked
    await f.stream.sink_consumer.consume(b'pending')
    await asyncio.sleep(.01)
    await stop(ds,.03)
    assert f.results and isinstance(f.results[0][0].error,TransportStopped)
    assert ds.connection.nc.is_closed and all(w.done() for w in ds.workers)


@pytest.mark.asyncio
async def test_native_publication_failure_is_typed(broker):
    f=setup(broker.url,js=True,createStream=True); sink_result_factory(f.stream,SinkHandler())
    ds=f.sinks[1]; await ds.start(Context())
    try:
        await broker.js.delete_stream('S')
        await f.stream.sink_consumer.consume(b'no-stream')
        await eventually(lambda: f.results)
        assert f.results[0][0].publication is None and f.results[0][0].error is not None
    finally: await stop(ds)


@pytest.mark.asyncio
async def test_two_core_queue_consumers_share_deliveries(broker):
    setups=[setup(broker.url,queueGroup='workers') for _ in range(2)]
    handlers=[SourceHandler(),SourceHandler()]
    for f,h in zip(setups,handlers):
        source_factory(f.stream,h); await f.sources[1].start(Context())
    try:
        for i in range(30): await broker.admin.publish('events',str(i).encode())
        await broker.admin.flush()
        await eventually(lambda: sum(len(h.ended) for h in handlers)==30)
        values=[value for f in setups for value,_ in f.received]
        assert len(values)==len(set(values))==30
        assert all(h.messages for h in handlers)
    finally:
        for f in setups: await stop(f.sources[1])


@pytest.mark.asyncio
async def test_source_bounded_pull_queue_and_shutdown_full(broker):
    f=setup(broker.url,js=True,durableName='D',createStream=True,createConsumer=True,result=True)
    handler=SourceHandler('wait'); source_factory(f.stream,handler)
    ds=f.sources[1]; await ds.start(Context())
    for i in range(100): await broker.js.publish('events',str(i).encode())
    consumer=ds.get_endpoint(2).consumer
    await eventually(lambda: consumer.queue.qsize()==64)
    assert len(consumer.active_owners)==1 and len(consumer.pending)==1
    await stop(ds,.03)
    info=await broker.js.consumer_info('S','D')
    assert info.ack_floor.stream_seq==0
    assert consumer.queue.empty() and all(t.done() for t in consumer.workers)


@pytest.mark.asyncio
async def test_core_overflow_is_reported_and_closes_admission(broker):
    f=setup(broker.url,result=True); handler=SourceHandler('wait')
    source_factory(f.stream,handler); ds=f.sources[1]; await ds.start(Context())
    try:
        for _ in range(300): await broker.admin.publish('events',b'work')
        await broker.admin.flush(); await eventually(lambda: handler.errors)
        consumer=ds.get_endpoint(2).consumer
        assert consumer.admission_closed and consumer.queue.qsize()<=64
    finally: await stop(ds,.03)


@pytest.mark.asyncio
async def test_sink_cancelled_before_admission_never_publishes(broker):
    from pyservicelib_gorundebug.runtime.context.request import request_context_scope
    f=setup(broker.url,js=True,createStream=True); handler=SinkHandler()
    sink_result_factory(f.stream,handler); ds=f.sinks[1]; await ds.start(Context())
    try:
        owner=Context(); owner.cancel()
        with request_context_scope(owner): await f.stream.sink_consumer.consume(b'cancelled')
        assert isinstance(handler.ended[0],AdmissionRejected)
        assert (await broker.js.stream_info('S')).state.messages==0
    finally: await stop(ds)


@pytest.mark.asyncio
async def test_tls_first_verified_server_and_hostname(broker,tmp_path):
    cert,key=tmp_path/'server.pem',tmp_path/'server.key'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
        '-keyout',str(key),'-out',str(cert),'-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],
        check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    await broker.admin.close(); broker.proc.terminate(); broker.proc.wait(timeout=5)
    conf=tmp_path/'tls.conf'
    conf.write_text(f'tls {{ cert_file: "{cert}", key_file: "{key}", handshake_first: true }}\n')
    broker.proc=subprocess.Popen(broker.command+['-c',str(conf)],stdout=broker.log,stderr=broker.log)
    await asyncio.sleep(.08)
    f=setup(broker.url); f.cfg.tls_enabled=True; f.cfg.tls_server_name='localhost'; f.cfg.tls_ca_file=str(cert)
    sink_result_factory(f.stream,SinkHandler(True)); ds=f.sinks[1]
    await ds.start(Context(timeout=timedelta(seconds=2)))
    try:
        await f.stream.sink_consumer.consume(b'TLS'); await eventually(lambda: f.results)
        assert f.results[0][0].error is None
    finally: await stop(ds)
    bad=setup(broker.url); bad.cfg.tls_enabled=True; bad.cfg.tls_server_name='wrong.example'; bad.cfg.tls_ca_file=str(cert)
    sink_result_factory(bad.stream,SinkHandler())
    with pytest.raises((TimeoutError,TransportStopped)):
        await bad.sinks[1].start(Context(timeout=timedelta(seconds=.15)))
    assert not bad.sinks[1].accepting

@pytest.mark.asyncio
@pytest.mark.parametrize('js', [False, True])
async def test_native_repeated_header_representation_limit(broker, js):
    """nats-py exposes scalar headers; document its wire-level duplicate boundary."""
    f = setup(broker.url, js=js, **(
        dict(createStream=True, createConsumer=True, durableName='header_limit') if js else {}))
    handler = SourceHandler('ack' if js else 'done')
    source_factory(f.stream, handler)
    ds = f.sources[1]
    await ds.start(Context())
    writer = None
    try:
        port = int(broker.url.rsplit(':', 1)[1])
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        assert (await asyncio.wait_for(reader.readline(), 2)).startswith(b'INFO ')
        # Inject actual wire duplicates: the public Python publisher itself accepts
        # only one string value per name. This test does not patch the client.
        headers = b'NATS/1.0\r\nX-Repeated: first\r\nX-Repeated: second\r\nx-stream-id: raw-header-limit\r\n\r\n'
        payload = b'\x00\xffpayload'
        writer.write(b'CONNECT {"verbose":false,"headers":true}\r\n' +
                     f'HPUB events {len(headers)} {len(headers)+len(payload)}\r\n'.encode() +
                     headers + payload + b'\r\nPING\r\n')
        await writer.drain()
        await eventually(lambda: handler.ended)
        assert handler.errors == [] and handler.ended == [None]
        assert handler.messages[0].data == payload
        assert handler.messages[0].headers['X-Repeated'] == 'second'
        assert f.received == [(payload, 'raw-header-limit')]
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        await stop(ds)

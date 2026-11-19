import pytest
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.operators.query import QueryStream
from pyservicelib_gorundebug.runtime.config.config import StreamConfig
from pyservicelib_gorundebug.runtime.config.stream_types import QueryStreamConfig
from pyservicelib_gorundebug.runtime.config.query import PostgreSQLSQLAlchemyDataConnectorConfig, QueryEndpointConfig
from pyservicelib_gorundebug.runtime.graph import runtime_to_stream_app
from .test_substream import LocalEnvironment, make_substream, echo

@pytest.mark.asyncio
async def test_query_disabled_and_live_graph():
    env = LocalEnvironment()
    _, source = make_substream(env, echo)
    cfg = env.config.config
    connector = PostgreSQLSQLAlchemyDataConnectorConfig(id=10, name='database', connectionString='postgres://user:secret@database/app')
    endpoint = QueryEndpointConfig(id=11, name='read', idDataConnector=10, queryOperation='Rows', fetchSize=64, enabled=False, functionName='Read')
    cfg.data_connectors.append(connector)
    cfg.endpoints.append(endpoint)
    query_config = StreamConfig(id=3, name='query', type=TransformationType.Query, idService=1, idSource=2, idEndpoint=11, valueType='int', xPos=0, yPos=0)
    cfg.streams.append(query_config)
    cfg.init_runtime_config()
    query = QueryStream[int, int, str](QueryStreamConfig(query_config), source)
    values = []
    class Handler:
        async def consume(self, value): values.append(value)
    query.set_query_consumer(Handler())
    await query.consume(1)
    assert values == []
    endpoint.enabled = True
    await query.consume(2)
    assert values == [2]
    graph = runtime_to_stream_app(env)
    assert len(graph.data_connectors) == 1
    assert len(graph.endpoints) == 1
    assert graph.endpoints[0].query_operation == 'Rows'
    assert graph.endpoints[0].fetch_size == 64
    assert 'secret' not in graph.to_json()
    assert graph.data_connectors[0].implementations is None
    assert connector.connection_string == 'postgres://user:secret@database/app'
    endpoint.query_operation = 'GetObject'
    with pytest.raises(ValueError, match='not supported'):
        cfg.init_runtime_config()

@pytest.mark.asyncio
async def test_query_completion_precedes_task_pool_delivery():
    import asyncio
    from datetime import timedelta
    from pyservicelib_gorundebug.dataquery import BeginRequest, make_endpoint_consumer
    from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
    from pyservicelib_gorundebug.runtime.config.config import LinkConfig, PoolConfig
    from pyservicelib_gorundebug.runtime.config import MapStreamConfig
    from pyservicelib_gorundebug.runtime.context.context import Context
    from pyservicelib_gorundebug.runtime.pool.taskpool import TaskPoolImpl
    from pyservicelib_gorundebug.operators.map import MapStream
    from pyservicelib_gorundebug.operators.functions import MapHandler
    env = LocalEnvironment()
    _, source = make_substream(env, echo)
    cfg = env.config.config
    cfg.data_connectors.append(PostgreSQLSQLAlchemyDataConnectorConfig(id=10, name='database', connectionString='postgres://unused/test'))
    cfg.endpoints.append(QueryEndpointConfig(id=11, name='read', idDataConnector=10, queryOperation='Rows', enabled=True, functionName='Read'))
    query_config = StreamConfig(id=3, name='query', type=TransformationType.Query, idService=1, idSource=2, idEndpoint=11, valueType='int', xPos=0, yPos=0)
    result_config = StreamConfig(id=4, name='result', type=TransformationType.Map, idService=1, idSource=3, valueType='int', xPos=0, yPos=0)
    cfg.streams.extend([query_config, result_config])
    cfg.pools.append(PoolConfig(name='Results', executorsCount=1))
    cfg.links.append(LinkConfig(var_from=3, to=4, callSemantics=CallSemantics.TaskPool, poolName='Results'))
    cfg.init_runtime_config()
    pool = TaskPoolImpl('Results', env)
    env._task_pools['Results'] = pool
    query = QueryStream[int, int, str](QueryStreamConfig(query_config), source)
    entered, release = asyncio.Event(), asyncio.Event()
    values, ended = [], []
    async def slow(_stream, value, _out):
        entered.set()
        await release.wait()
        values.append(value)
    MapStream[int, int](MapStreamConfig(result_config), query, MapHandler(slow))
    class Handler:
        async def begin_request(self, stream): return BeginRequest(None)
        async def end_request(self, stream, state, error, out, errors):
            assert error is None
            ended.append(True)
            await out.out(99)
    async def execute(state, value, out): await out.out(value)
    make_endpoint_consumer(query, Handler(), execute)
    context = Context(timedelta(seconds=3))
    await pool.start(context)
    try:
        async with asyncio.timeout(3):
            await query.consume(42)
            await entered.wait()
            assert ended == [True]
            assert values == []
    finally:
        release.set()
        await pool.stop(context)
    assert values == [42, 99]

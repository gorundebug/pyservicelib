import asyncio

import pytest

from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics
from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.operators.functions import MapHandler
from pyservicelib_gorundebug.operators.map import MapStream
from pyservicelib_gorundebug.operators.merge import MergeStream
from pyservicelib_gorundebug.operators.split import SplitStream
from pyservicelib_gorundebug.operators.substream import SubStream
from pyservicelib_gorundebug.runtime.common import Collect, Stream, SubStreamCollectorFunc
from pyservicelib_gorundebug.runtime.config import (
    LinkConfig, MapStreamConfig, MergeStreamConfig, PoolConfig,
    SplitStreamConfig, StreamConfig, SubStreamConfig,
)
from pyservicelib_gorundebug.runtime.context import default_context
from pyservicelib_gorundebug.runtime.context.request import (
    request_priority, request_stream_id,
)
from pyservicelib_gorundebug.runtime.pool.prioritytaskpool import PriorityTaskPoolImpl
from pyservicelib_gorundebug.runtime.pool.taskpool import TaskPoolImpl

from .test_substream import LocalEnvironment


@pytest.mark.asyncio
@pytest.mark.parametrize("workers", [1, 4])
async def test_request_context_across_split_pools_parallel_and_merge(workers: int) -> None:
    env = LocalEnvironment()
    cfg = env.config.config
    definitions = [
        (1, TransformationType.SubStream, 9),
        (2, TransformationType.Split, 1),
        *((index, TransformationType.Map, 2) for index in range(3, 8)),
        (8, TransformationType.Merge, 3),
        (9, TransformationType.Map, 8),
    ]
    configs = {
        index: StreamConfig(
            id=index, name=f"context-{index}", type=kind, idService=1,
            idSource=source, valueType="int", xPos=0, yPos=0,
        )
        for index, kind, source in definitions
    }
    cfg.streams.extend(configs.values())
    cfg.pools.extend([
        PoolConfig(name="fifo", executorsCount=workers),
        PoolConfig(name="priority", executorsCount=workers),
    ])
    modes = [
        CallSemantics.FunctionCall, CallSemantics.FunctionCall,
        CallSemantics.TaskPool, CallSemantics.PriorityTaskPool,
        CallSemantics.ParallelCall,
    ]
    cfg.links.extend([
        LinkConfig(var_from=2, to=index + 3, callSemantics=mode,
                   poolName=("fifo" if index == 2 else "priority")
                   if index in (2, 3) else None,
                   priority=0 if index == 3 else None,
                   **{"async": index == 1})
        for index, mode in enumerate(modes)
    ])
    cfg.links.append(LinkConfig(
        var_from=8, to=9, callSemantics=CallSemantics.TaskPool, poolName="fifo",
    ))
    cfg.init_runtime_config()
    fifo = TaskPoolImpl("fifo", env)
    priority = PriorityTaskPoolImpl("priority", env)
    env._task_pools["fifo"] = fifo
    env._priority_task_pools["priority"] = priority
    entry = SubStream[int, int](SubStreamConfig(configs[1]), env)
    split = SplitStream[int](SplitStreamConfig(configs[2]), entry)
    observations: list[tuple[str, int, str | None, int | None]] = []

    def observe(stage: str, request: int) -> None:
        observations.append((stage, request, request_stream_id.get(), request_priority.get()))

    def branch_handler(index: int) -> MapHandler[int, int]:
        async def process(stream: Stream, value: int, out: Collect[int]) -> None:
            observe(f"branch-{index}-before", value)
            await asyncio.sleep(0)
            observe(f"branch-{index}-after", value)
            await out.out(value * 10 + index)
            observe(f"branch-{index}-returned", value)
        return MapHandler(process)

    branches = [
        MapStream[int, int](MapStreamConfig(configs[index + 3]),
                            split.add_stream(), branch_handler(index))
        for index in range(5)
    ]
    merged = MergeStream[int](MergeStreamConfig(configs[8]), *branches)

    async def terminal(stream: Stream, value: int, out: Collect[int]) -> None:
        observe("after-merge-before", value // 10)
        await asyncio.sleep(0)
        observe("after-merge-after", value // 10)
        await out.out(value)

    result = MapStream[int, int](MapStreamConfig(configs[9]), merged, MapHandler(terminal))
    entry.set_source(result)
    split.build()
    entry.build()
    bootstrap = request_stream_id.set("worker-bootstrap")
    try:
        await fifo.start(default_context())
        await priority.start(default_context())
    finally:
        request_stream_id.reset(bootstrap)

    async def invoke(request: int) -> list[int]:
        results: list[int] = []
        sid_token = request_stream_id.set(f"request-{request}")
        priority_token = request_priority.set(request % 3)

        async def collect(value: int) -> bool:
            observe("collector", request)
            results.append(value)
            await asyncio.sleep(0)
            observe("collector-after", request)
            return len(results) == 5

        try:
            observe("entry", request)
            await entry.consume(request, SubStreamCollectorFunc(collect))
            observe("caller-returned", request)
            return results
        finally:
            request_priority.reset(priority_token)
            request_stream_id.reset(sid_token)

    caller_id = request_stream_id.get()
    caller_priority = request_priority.get()
    try:
        results = await asyncio.wait_for(asyncio.gather(*(invoke(i) for i in range(100))), 10)
        if env._tasks:
            await asyncio.wait_for(asyncio.gather(*tuple(env._tasks)), 5)
    finally:
        await asyncio.wait_for(fifo.stop(default_context()), 5)
        await asyncio.wait_for(priority.stop(default_context()), 5)

    assert request_stream_id.get() == caller_id
    assert request_priority.get() == caller_priority
    assert results and all(sorted(values) == [i * 10 + j for j in range(5)]
                           for i, values in enumerate(results))
    assert len(observations) == 100 * (2 + 5 * 7)
    mismatches = [item for item in observations
                  if item[2] != f"request-{item[1]}" or item[3] != item[1] % 3]
    assert not mismatches, mismatches[:20]

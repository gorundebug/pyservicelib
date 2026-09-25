from typing import Any
from types import SimpleNamespace

import pytest

from pyservicelib_gorundebug.datasink.grpc.grpcds import _GrpcSinkEndpoint
from pyservicelib_gorundebug.datasink.http.aiohttpds import _AIOHttpSinkEndpoint
from pyservicelib_gorundebug.datasink.kafka.aiokafkads import _AIOKafkaSinkEndpoint
from pyservicelib_gorundebug.runtime.context import Context, default_context


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_second", [False, True])
@pytest.mark.parametrize("endpoint_type", [_GrpcSinkEndpoint, _AIOHttpSinkEndpoint, _AIOKafkaSinkEndpoint])
async def test_grpc_endpoint_owns_all_consumers_lifecycle(fail_second: bool, endpoint_type: Any) -> None:
    events: list[str] = []

    class Consumer:
        def __init__(self, name: str) -> None:
            self.name = name

        async def start(self, ctx: Context) -> None:
            events.append(f"start:{self.name}")
            if fail_second and self.name == "second":
                raise RuntimeError("second failed")

        async def stop(self, ctx: Context) -> None:
            events.append(f"stop:{self.name}")

    # Isolate lifecycle dispatch from network setup, preserving the actual
    # endpoint registry populated when sink consumers are constructed.
    endpoint: Any = object.__new__(endpoint_type)
    endpoint._id = 1
    endpoint._data_sink = SimpleNamespace(environment=SimpleNamespace(config=SimpleNamespace(
        get_endpoint_config_by_id=lambda _: SimpleNamespace(enabled=True),
    )))
    endpoint._endpoint_consumers = [Consumer("first"), Consumer("second"), Consumer("third")]
    endpoint._consumer_obj = endpoint._endpoint_consumers[-1]
    endpoint._consumer = endpoint._endpoint_consumers[-1]
    if fail_second:
        with pytest.raises(RuntimeError, match="second failed"):
            await endpoint.start(default_context())
        assert events == ["start:first", "start:second", "stop:first"]
    else:
        await endpoint.start(default_context())
        await endpoint.stop(default_context())
        assert events == [
            "start:first", "start:second", "start:third",
            "stop:third", "stop:second", "stop:first",
        ]

from types import SimpleNamespace

from pyservicelib_gorundebug.api.models.transformation_type import TransformationType
from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
from pyservicelib_gorundebug.runtime.config.config import StreamConfig
from pyservicelib_gorundebug.runtime.graph import runtime_to_stream_app


def test_runtime_graph_reconstructs_virtual_error_output() -> None:
    owner_config = StreamConfig(
        id=7,
        name="Process Order",
        idSource=1,
        type=TransformationType.Process,
        valueType="OrderResult",
        idService=3,
        xPos=20,
        yPos=30,
        pipeline="orders",
    )
    consumer_config = StreamConfig(
        id=8,
        name="Map Failure",
        idSource=-7,
        type=TransformationType.Map,
        valueType="OrderResult",
        idService=3,
        xPos=40,
        yPos=50,
        pipeline="orders",
    )
    streams = {
        7: SimpleNamespace(config=owner_config),
        8: SimpleNamespace(config=consumer_config),
    }
    config = SimpleNamespace(
        services=[],
        settings=ProjectSettings(name="Test"),
        types=[],
        modules=None,
        get_link=lambda *_: None,
    )
    app = SimpleNamespace(
        config=config,
        service_config=None,
        _streams=streams,
        _task_pools={},
        _priority_task_pools={},
        _dataSources={},
        _dataSinks={},
        _runtime_links=[],
    )

    graph = runtime_to_stream_app(app)

    by_id = {stream.id: stream for stream in graph.streams}
    assert by_id[-7].type == TransformationType.Error
    assert by_id[-7].id_source == 7
    assert by_id[-7].name == "Process Order Error"
    assert by_id[8].id_source == -7

from types import SimpleNamespace

import pytest
import yaml
from pyservicelib_gorundebug.api.models.environment import Environment
from pyservicelib_gorundebug.api.models.service import Service
from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
from pyservicelib_gorundebug.api.models.stream_app import StreamApp
from pyservicelib_gorundebug.runtime.common import RuntimeHelpers, ServiceStream
from pyservicelib_gorundebug.runtime.config import StreamConfig
from pyservicelib_gorundebug.runtime.config.config_to_api import stream_config_to_api
from pyservicelib_gorundebug.runtime.config.app_to_yaml import app_to_yaml
from pyservicelib_gorundebug.runtime.config.stream_types import MapStreamConfig
from pyservicelib_gorundebug.runtime.environment.tracing import NOOP_SPAN, sampling_scope, string_attr
from pyservicelib_gorundebug.runtime.environment.tracing.tracing import start_stream_span, start_endpoint_span
from pyservicelib_gorundebug.runtime.testmetrics.testmetrics import TestMetrics as Metrics
from pyservicelib_gorundebug.api.models.call_semantics import CallSemantics


def config(component="Customer Pricing"):
    return StreamConfig(id=2, name="Calculate Price", idSource=1, type=2, idService=1,
                        xPos=0, yPos=0, valueType="Amount", pipeline="pricing", component=component)


class RecordingTracer:
    def __init__(self):
        self.spans = []

    def start(self, name, *attrs):
        self.spans.append((name, {attr.key: attr.value for attr in attrs}))
        return None, NOOP_SPAN


@pytest.mark.asyncio
@pytest.mark.parametrize("semantics", [CallSemantics.FunctionCall, CallSemantics.TaskPool,
                                        CallSemantics.PriorityTaskPool, CallSemantics.ParallelCall])
async def test_existing_link_counter_and_span_use_receiving_grouping(semantics):
    values, tasks = [], []
    tracer, metrics = RecordingTracer(), Metrics()

    async def consume(value):
        values.append(value)

    class Pool:
        name = "Workers"
        async def add_task(self, *args):
            await args[-1]()

    target = SimpleNamespace(id=2, name="Calculate Price", config=config())
    consumer = SimpleNamespace(stream=target, consume=consume)
    link = SimpleNamespace(call_semantics=semantics, pool_name="Workers", priority=7, var_async=False)
    runtime = SimpleNamespace(register_consume_statistics=lambda *args: None,
                              register_link_info=lambda *args: None,
                              get_task_pool=lambda name: Pool(), get_priority_task_pool=lambda name: Pool())
    environment = SimpleNamespace(config=SimpleNamespace(get_link=lambda *args: link), runtime=runtime,
                                  service_config=SimpleNamespace(id=1, name="Booking", default_call_semantics=semantics),
                                  metrics=metrics, tracing=SimpleNamespace(tracer=lambda name: tracer),
                                  create_task=lambda fn, *args: tasks.append(fn(*args)))
    source = SimpleNamespace(id=1, name="Input", consumer=consumer, environment=environment,
                             config=SimpleNamespace(id_service=1, pipeline="entry", component="Request"))
    caller = RuntimeHelpers(environment).make_caller(source)
    with sampling_scope(True):
        await caller.consume(42)
        for task in tasks:
            await task
    assert values == [42]
    labels = {"service":"Booking", "from":"Input", "to":"Calculate Price", "pipeline":"pricing", "component":"Customer Pricing"}
    assert metrics.counter("stream_messages_total", labels).count() == 1
    assert len(tracer.spans) == 1
    name, attrs = tracer.spans[0]
    assert name == "stream.call"
    assert attrs["pipeline"] == "pricing"
    assert attrs["component"] == "Customer Pricing"
    assert "component_instance" not in attrs


def test_operator_and_endpoint_grouping_respects_sampling_fast_path():
    tracer = RecordingTracer()
    stream = SimpleNamespace(name="Calculate Price", config=config())
    stream.trace_attributes = (string_attr("stream", stream.name), string_attr("pipeline", "pricing"),
                               string_attr("component", "Customer Pricing"))
    with sampling_scope(True):
        start_stream_span(tracer, "stream.map", stream)
        start_endpoint_span(tracer, "http.output", stream.name, "Route", "method", "POST",
                            pipeline_name="pricing", component_name="Customer Pricing")
    for _, attrs in tracer.spans:
        assert attrs["pipeline"] == "pricing"
        assert attrs["component"] == "Customer Pricing"
        assert "component_instance" not in attrs
    assert tracer.spans[1][1]["method"] == "POST"

    class Unresolved:
        @property
        def config(self):
            raise AssertionError("unsampled path must not resolve grouping")
    with sampling_scope(False):
        assert start_stream_span(tracer, "stream.map", Unresolved())[1] is NOOP_SPAN
        assert start_endpoint_span(tracer, "http.output", "Unresolved", "Route", pipeline_name="", component_name="")[1] is NOOP_SPAN
    assert len(tracer.spans) == 2


@pytest.mark.parametrize("component", ["Customer Pricing", None])
def test_config_api_and_yaml_preserve_optional_component(component):
    cfg = config(component)
    assert MapStreamConfig(cfg).component == component
    api = stream_config_to_api(cfg)
    assert api.component == component
    assert type(api).from_dict(api.to_dict()).component == component
    service = Service(id=1, name="Booking", defaultCallSemantics=2, programmingLanguage=3,
                      modulePath="booking", httpHost="", httpPort=0, grpcHost="", grpcPort=9200,
                      shutdownTimeout=1000, environment=next(iter(Environment)), color="#000000",
                      statusHandler="", metricsHandler="", startupHandler="", readinessHandler="",
                      livenessHandler="", kubernetesWorkloadType="Deployment", defaultGrpcTimeout=0)
    app = StreamApp(settings=ProjectSettings(name="Booking"), services=[service], streams=[api],
                    links=[], types=[], dataConnectors=[], endpoints=[], pools=[])
    doc = yaml.safe_load(app_to_yaml(app))
    written = doc["services"]["booking"]["pipelines"]["pricing"]["calculatePrice"]
    assert written.get("component") == component
    assert "component_instance" not in written


@pytest.mark.parametrize("enabled", [True, False])
def test_endpoint_span_accepts_only_explicit_attributes(enabled):
    tracer = RecordingTracer()
    with sampling_scope(enabled):
        start_endpoint_span(
            tracer, "http.input", "Receive Booking", "Bookings",
            "method", "POST", "path", "/bookings",
            pipeline_name="booking", component_name="Validate Request",
        )
    if enabled:
        assert tracer.spans == [("http.input", {
            "stream": "Receive Booking", "endpoint": "Bookings",
            "method": "POST", "path": "/bookings", "pipeline": "booking",
            "component": "Validate Request",
        })]
    else:
        assert tracer.spans == []


def test_endpoint_span_keeps_fixed_parameters_and_does_not_invent_grouping():
    import inspect
    assert all(parameter.kind not in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
               for parameter in inspect.signature(start_endpoint_span).parameters.values())
    tracer = RecordingTracer()
    with sampling_scope(True):
        start_endpoint_span(tracer, "http.input", "Input", "Route")
    assert tracer.spans == [("http.input", {"stream": "Input", "endpoint": "Route"})]


@pytest.mark.parametrize("tracer_present, sampled", [(False, False), (False, True), (True, False)])
def test_disabled_endpoint_tracing_creates_neither_attributes_nor_spans(monkeypatch, tracer_present, sampled):
    import importlib
    helpers = importlib.import_module("pyservicelib_gorundebug.runtime.environment.tracing.tracing")
    def unexpected(*args, **kwargs):
        raise AssertionError("disabled tracing must not create attributes or start spans")
    tracer = SimpleNamespace(start=unexpected) if tracer_present else None
    monkeypatch.setattr(helpers, "string_attr", unexpected)
    with sampling_scope(sampled):
        assert start_endpoint_span(
            tracer, "http.input", "Input", "Route", "method", "POST", "path", "/bookings",
            pipeline_name="booking", component_name="Validate Request",
        ) == (None, NOOP_SPAN)


def test_operator_spans_reuse_attributes_without_reading_config(monkeypatch):
    import importlib
    helpers = importlib.import_module("pyservicelib_gorundebug.runtime.environment.tracing.tracing")

    class ConcreteStream(ServiceStream):
        @property
        def type_name(self):
            return "Amount"

    cfg = SimpleNamespace(name="Price", transformation_name="Map", pipeline="pricing",
                          component="Customer Pricing")
    reads = []
    def read_config(stream_id):
        reads.append(stream_id)
        assert len(reads) == 1, "operator spans must not look up configuration"
        return cfg
    environment = SimpleNamespace(config=SimpleNamespace(get_stream_config_by_id=read_config),
                                  runtime=SimpleNamespace(register_stream=lambda stream: None))
    stream = ConcreteStream(2, environment)
    attributes = stream.trace_attributes
    assert stream.trace_attributes is attributes

    class IdentityTracer:
        def start(self, operation, *attrs):
            assert operation == "stream.map"
            assert all(actual is cached for actual, cached in zip(attrs, attributes, strict=True))
            return None, NOOP_SPAN

    def allocate_attribute(*args):
        raise AssertionError("enabled operator tracing must reuse cached attributes")
    monkeypatch.setattr(helpers, "string_attr", allocate_attribute)
    with sampling_scope(True):
        for _ in range(100):
            start_stream_span(IdentityTracer(), "stream.map", stream)
    assert reads == [2]

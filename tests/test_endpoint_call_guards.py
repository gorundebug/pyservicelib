"""Exercise the actual transport call blocks before helper argument evaluation."""

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from pyservicelib_gorundebug.runtime.environment.tracing import NOOP_SPAN, sampling_scope, sampling_enabled


ROOT = Path(__file__).parents[1] / "src" / "pyservicelib_gorundebug"


def endpoint_calls():
    cases = []
    for folder in ("datasource", "datasink"):
        for path in sorted((ROOT / folder).rglob("*.py")):
            tree = ast.parse(path.read_text())
            parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
            for call in ast.walk(tree):
                if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
                    continue
                if call.func.id != "start_endpoint_span":
                    continue
                assignment = parents[call]
                guard = parents[assignment]
                assert isinstance(guard, ast.If), f"Unguarded endpoint span: {path}:{call.lineno}"
                assert assignment in guard.body, f"Span call is not in the enabled branch: {path}"
                code = compile(ast.Module(body=[guard], type_ignores=[]), str(path), "exec")
                cases.append(pytest.param(code, id=f"{path.relative_to(ROOT)}:{call.lineno}"))
    assert len(cases) >= 13, "Transport span call coverage unexpectedly disappeared"
    return cases


@pytest.mark.parametrize("code", endpoint_calls())
@pytest.mark.parametrize("tracer_present,sampled", [(False, False), (False, True), (True, False), (True, True)])
def test_endpoint_call_and_arguments_are_skipped_before_helper(code, tracer_present, sampled):
    touched = []
    called = []

    class Metadata:
        def __getattr__(self, name):
            touched.append(name)
            if not tracer_present or not sampled:
                raise AssertionError(f"Disabled tracing read metadata: {name}")
            return self

    tracer = object() if tracer_present else None
    metadata = Metadata()

    class Consumer(Metadata):
        _tracer = tracer

    def start(*args, **kwargs):
        called.append((args, kwargs))
        return None, span

    def sample():
        assert tracer_present, "No tracer must short-circuit the sampling lookup"
        return sampling_enabled()

    span = object()
    scope = dict(self=Consumer(), stream=metadata, ep=metadata, sid="request-id",
                 span=NOOP_SPAN, sampling_enabled=sample, start_endpoint_span=start)
    with sampling_scope(sampled):
        exec(code, scope)
    if tracer_present and sampled:
        assert len(called) == 1
        assert touched
        assert scope["span"] is span
    else:
        assert not called
        assert not touched
        assert scope["span"] is NOOP_SPAN

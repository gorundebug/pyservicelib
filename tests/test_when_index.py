from types import SimpleNamespace
from typing import cast

import pytest

from pyservicelib_gorundebug.operators.functions import When, when_index


@pytest.mark.parametrize("names", [
    ["accepted", "terminal"],
    ["terminal", "accepted"],
    ["unrelated", "terminal", "accepted"],
])
def test_when_index_preserves_same_typed_outcomes(names: list[str]) -> None:
    items = [cast(When, SimpleNamespace(name=name, type=bool)) for name in names]
    accepted = when_index(items, "accepted")
    terminal = when_index(items, "terminal")
    select = lambda value: terminal if value else accepted
    assert items[select(False)].name == "accepted"
    assert items[select(True)].name == "terminal"


@pytest.mark.parametrize("names, target, error", [
    (["accepted"], "terminal", "not found"),
    (["terminal", "terminal"], "terminal", "ambiguous"),
    ([], "", "must not be empty"),
])
def test_when_index_rejects_invalid_binding(names: list[str], target: str, error: str) -> None:
    items = [cast(When, SimpleNamespace(name=name, type=bool)) for name in names]
    with pytest.raises(ValueError, match=error):
        when_index(items, target)

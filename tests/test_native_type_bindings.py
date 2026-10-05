from typing import Any

import pytest
import yaml

from pyservicelib_gorundebug.api.models.project_settings import ProjectSettings
from pyservicelib_gorundebug.api.models.stream_app import StreamApp
from pyservicelib_gorundebug.api.models.type import Type
from pyservicelib_gorundebug.runtime.config.app_to_yaml import app_to_yaml
from pyservicelib_gorundebug.runtime.config.config import TypeConfig
from pyservicelib_gorundebug.runtime.config.config_to_api import type_config_to_api


@pytest.mark.parametrize("bindings", [
    None,
    {},
    {
        "python": {"definition": "dict[str, int]", "import": "app.types"},
        "cppUserver": {"definition": "UserverValue"},
        "cppCoro": {"definition": "CoroValue"},
        "new-runtime": {"definition": "", "import": "", "package": "sample"},
        "empty": {},
    },
])
def test_native_bindings_survive_runtime_api_and_yaml(
    bindings: dict[str, dict[str, str]] | None,
) -> None:
    document: dict[str, Any] = {"name": "Opaque", "type": "custom"}
    if bindings is not None:
        document["bindings"] = bindings
    config = TypeConfig.model_validate(document)
    api_type = type_config_to_api(config)
    exported = api_type.to_dict()
    if bindings is None:
        assert "bindings" not in exported
    else:
        assert exported["bindings"] == bindings
    restored = Type.from_dict(exported)
    assert restored is not None
    assert restored.to_dict() == exported

    app = StreamApp(
        settings=ProjectSettings(name="native-bindings"),
        streams=[], services=[], links=[], types=[api_type],
        dataConnectors=[], endpoints=[], pools=[],
    )
    output = yaml.safe_load(app_to_yaml(app))["types"]["opaque"]
    if bindings is None:
        assert "bindings" not in output
    else:
        assert output["bindings"] == bindings
    assert not any(key.startswith(("typeDefinitionLang", "typeImportLang")) for key in output)


def test_native_binding_api_has_no_retired_language_slots() -> None:
    assert "bindings" in Type.model_fields
    assert not any(
        name.startswith(("type_definition_lang", "type_import_lang"))
        for name in Type.model_fields
    )

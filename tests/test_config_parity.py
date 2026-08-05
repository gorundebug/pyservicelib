from pyservicelib_gorundebug.runtime.config.config import ModuleConfig


def test_module_config_accepts_go_runtime_path_shape() -> None:
    config = ModuleConfig.from_dict({"name": "shared", "path": "example/shared"})

    assert config is not None
    assert config.name == "shared"
    assert config.module_path == "example/shared"
    assert config.golang_version == ""

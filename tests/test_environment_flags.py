from pyservicelib_gorundebug.runtime.environment.flags import flag_enabled


def test_environment_flags_use_strict_boolean_values(monkeypatch) -> None:
    name = "SERVICELIB_TEST_BOOLEAN_FLAG"
    for value in ("1", "true", "TRUE", " yes ", "On"):
        monkeypatch.setenv(name, value)
        assert flag_enabled(name)
    for value in ("", "0", "false", "no", "off", "anything"):
        monkeypatch.setenv(name, value)
        assert not flag_enabled(name)
    monkeypatch.delenv(name)
    assert not flag_enabled(name)

from pyservicelib_gorundebug.runtime.environment.log import (
    FieldType,
    Level,
    bool_field,
    err_field,
    float64_field,
    int64_field,
    str_field,
)
from pyservicelib_gorundebug.runtime.testlog import TestLog


def test_structured_log_level_and_typed_field_contract() -> None:
    engine = TestLog()
    logger = engine.default_logger()
    logger.debug("debug event")
    logger.info("info event")
    logger.warn(
        "request failed",
        str_field("endpoint", "orders"),
        int64_field("attempt", 2),
        float64_field("ratio", 1.5),
        bool_field("retry", True),
    )
    logger.error("shutdown failed", err_field(RuntimeError("timeout")))

    entries = engine.entries()
    assert [entry.level for entry in entries] == [
        Level.DEBUG,
        Level.INFO,
        Level.WARN,
        Level.ERROR,
    ]
    fields = entries[2].fields
    assert [(field.key, field.type, field.value()) for field in fields] == [
        ("endpoint", FieldType.STRING, "orders"),
        ("attempt", FieldType.INT64, 2),
        ("ratio", FieldType.FLOAT64, 1.5),
        ("retry", FieldType.BOOL, True),
    ]
    error = entries[3].fields[0]
    assert error.key == "error"
    assert error.type == FieldType.ERROR
    assert error.string_value() == "timeout"
    assert len(engine.entries_at_level(Level.ERROR)) == 1
    engine.reset()
    assert engine.entries() == []

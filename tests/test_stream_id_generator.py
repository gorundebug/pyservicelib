import uuid
from pyservicelib_gorundebug.runtime.context.request import new_stream_id


def test_stream_ids_keep_uuid4_format_version_variant_and_uniqueness():
    values = {new_stream_id() for _ in range(10000)}
    assert len(values) == 10000
    for value in values:
        parsed = uuid.UUID(value)
        assert parsed.version == 4
        assert parsed.variant == uuid.RFC_4122
        assert str(parsed) == value

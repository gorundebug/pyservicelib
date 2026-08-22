from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiokafka.errors import (  # type: ignore[import-untyped]
    InvalidPartitionsError,
    TopicAlreadyExistsError,
)

from pyservicelib_gorundebug.datasink.kafka import aiokafkads as sink_kafka
from pyservicelib_gorundebug.datasource.kafka import aiokafkads as source_kafka


@pytest.mark.parametrize("module", [sink_kafka, source_kafka])
def test_kafka_client_options_apply_dial_timeout(module: object) -> None:
    options = module._client_options(  # type: ignore[attr-defined]
        SimpleNamespace(dial_timeout=1250.8, version="2.6.0")
    )

    assert options == {
        "request_timeout_ms": 1250,
        "security_protocol": "PLAINTEXT",
    }


@pytest.mark.parametrize("module", [sink_kafka, source_kafka])
@pytest.mark.asyncio
async def test_create_topics_uses_endpoint_configuration(
    monkeypatch: pytest.MonkeyPatch, module: object
) -> None:
    admin = SimpleNamespace(
        start=AsyncMock(),
        create_topics=AsyncMock(
            return_value=SimpleNamespace(topic_errors=[("events", 0, None)])
        ),
        close=AsyncMock(),
    )
    monkeypatch.setattr(module, "AIOKafkaAdminClient", lambda **kwargs: admin)

    await module._create_topics(  # type: ignore[attr-defined]
        "broker:9092",
        [
            SimpleNamespace(
                create_topic=True,
                topic="events",
                partitions=3,
                replication_factor=2,
            )
        ],
    )

    admin.start.assert_awaited_once()
    topics = admin.create_topics.await_args.args[0]
    assert len(topics) == 1
    assert topics[0].name == "events"
    assert topics[0].num_partitions == 3
    assert topics[0].replication_factor == 2
    admin.close.assert_awaited_once()


@pytest.mark.parametrize("module", [sink_kafka, source_kafka])
@pytest.mark.asyncio
async def test_create_topics_skips_disabled_request(
    monkeypatch: pytest.MonkeyPatch, module: object
) -> None:
    factory = AsyncMock()
    monkeypatch.setattr(module, "AIOKafkaAdminClient", factory)

    await module._create_topics(  # type: ignore[attr-defined]
        "broker:9092",
        [SimpleNamespace(create_topic=False, topic="events")],
    )

    factory.assert_not_called()


@pytest.mark.parametrize("module", [sink_kafka, source_kafka])
@pytest.mark.asyncio
async def test_create_topics_accepts_existing_topic(
    monkeypatch: pytest.MonkeyPatch, module: object
) -> None:
    admin = SimpleNamespace(
        start=AsyncMock(),
        create_topics=AsyncMock(
            return_value=SimpleNamespace(
                topic_errors=[("events", TopicAlreadyExistsError.errno, "exists")]
            )
        ),
        close=AsyncMock(),
    )
    monkeypatch.setattr(module, "AIOKafkaAdminClient", lambda **kwargs: admin)

    await module._create_topics(  # type: ignore[attr-defined]
        "broker:9092",
        [SimpleNamespace(create_topic=True, topic="events")],
    )

    admin.close.assert_awaited_once()


@pytest.mark.parametrize("module", [sink_kafka, source_kafka])
@pytest.mark.asyncio
async def test_create_topics_reports_broker_error(
    monkeypatch: pytest.MonkeyPatch, module: object
) -> None:
    admin = SimpleNamespace(
        start=AsyncMock(),
        create_topics=AsyncMock(
            return_value=SimpleNamespace(
                topic_errors=[("events", InvalidPartitionsError.errno, "invalid")]
            )
        ),
        close=AsyncMock(),
    )
    monkeypatch.setattr(module, "AIOKafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(InvalidPartitionsError):
        await module._create_topics(  # type: ignore[attr-defined]
            "broker:9092",
            [SimpleNamespace(create_topic=True, topic="events")],
        )

    admin.close.assert_awaited_once()

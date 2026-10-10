import asyncio
import logging
from collections.abc import Awaitable, Callable

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import GroupCoordinatorNotAvailableError, InvalidPartitionsError
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics import consumer as consumer_module
from order_analytics.config import Settings
from order_analytics.consumer import GROUP_ID, EventConsumer
from order_analytics.ingest import Message, Outcome, ingest
from order_analytics.main import create_app
from order_analytics.seed import (
    ANA,
    KEYBOARD,
    Ending,
    ExampleOrder,
    SeedMessage,
    seed_messages,
    story,
)
from order_analytics.tables import Event, RejectedEvent

Sessions = async_sessionmaker[AsyncSession]


async def publish(bootstrap_servers: str, messages: list[SeedMessage]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for message in messages:
            await producer.send_and_wait(message.topic, value=message.value, key=message.key)
    finally:
        await producer.stop()


async def committed_offsets(bootstrap_servers: str) -> int:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(GROUP_ID)
    except GroupCoordinatorNotAvailableError:
        # A broker that has just started is still creating its offsets topic.
        return 0
    finally:
        await admin.close()
    return sum(committed.offset for committed in offsets.values())


async def count(sessions: Sessions, table: type) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(table)) or 0


async def eventually(condition: Callable[[], Awaitable[bool]], seconds: float = 60) -> None:
    async with asyncio.timeout(seconds):
        # The condition lives in Kafka and PostgreSQL: there is no event to wait on, only polling.
        while not await condition():  # noqa: ASYNC110
            await asyncio.sleep(0.05)


async def test_service_turns_what_is_published_on_the_three_topics_into_numbers(
    kafka: str, database_url: str, sessions: Sessions
) -> None:
    messages = seed_messages()
    events = len(messages) - 2  # the seed repeats one event and sends one broken message

    async def all_received() -> bool:
        return await committed_offsets(kafka) == len(messages)

    app = create_app(Settings(database_url=database_url, kafka_bootstrap_servers=kafka))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app), base_url="http://test") as http,
    ):
        await publish(kafka, messages)
        await eventually(all_received)

        assert (await http.get("/health")).json() == {
            "status": "UP",
            "database": "UP",
            "consumer": "UP",
        }
        summary = (await http.get("/analytics/summary")).json()

    assert await count(sessions, Event) == events
    assert await count(sessions, RejectedEvent) == 1
    assert summary == {
        "totalOrders": 16,
        "confirmed": 9,
        "cancelled": 5,
        "inProgress": 2,
        "confirmedRevenue": 7871.9,
        "approvalRate": 0.6429,
        "averageSecondsToConfirm": 6.0,
        "lastEventAt": "2026-10-09T09:31:00Z",
    }


async def test_stop_in_the_middle_commits_what_was_stored_and_the_restart_reads_only_the_rest(
    kafka: str, sessions: Sessions, caplog: pytest.LogCaptureFixture
) -> None:
    order = ExampleOrder("2026-10-05T09:15:00", ANA, ((KEYBOARD, 1),), Ending.CONFIRMED, 4)
    messages = [message for number in range(150) for message in story(f"order-{number}", order)]
    await publish(kafka, messages)

    async def some_stored() -> bool:
        return await count(sessions, Event) > 0

    async def all_stored() -> bool:
        return await count(sessions, Event) == len(messages)

    first = EventConsumer(kafka, sessions)
    first.start()
    await eventually(some_stored)
    await first.stop()

    stored_by_first = await count(sessions, Event)
    assert 0 < stored_by_first < len(messages)
    assert await committed_offsets(kafka) == stored_by_first

    with caplog.at_level(logging.INFO, logger="order_analytics.ingest"):
        second = EventConsumer(kafka, sessions)
        second.start()
        await eventually(all_stored)
        await second.stop()

    assert await committed_offsets(kafka) == len(messages)
    assert "duplicate event ignored" not in caplog.text
    assert caplog.text.count("event stored") == len(messages) - stored_by_first


async def test_message_that_could_not_be_stored_is_read_again_instead_of_skipped(
    kafka: str, sessions: Sessions, monkeypatch: pytest.MonkeyPatch
) -> None:
    order = ExampleOrder("2026-10-05T09:15:00", ANA, ((KEYBOARD, 1),), Ending.CONFIRMED, 4)
    messages = story("order-1", order)
    reserved = messages[1]
    await publish(kafka, messages)
    failures_left = 2

    async def database_away_twice(sessions: Sessions, message: Message) -> Outcome:
        nonlocal failures_left
        if message.value == reserved.value and failures_left:
            failures_left -= 1
            raise OSError("connection refused")
        return await ingest(sessions, message)

    async def all_committed() -> bool:
        return await committed_offsets(kafka) == len(messages)

    monkeypatch.setattr(consumer_module, "ingest", database_away_twice)
    monkeypatch.setattr(consumer_module, "_RETRY_SECONDS", 0.1)
    consumer = EventConsumer(kafka, sessions)
    consumer.start()
    await eventually(all_committed)
    await consumer.stop()

    assert failures_left == 0
    async with sessions() as session:
        stored = (await session.scalars(select(Event.type).order_by(Event.occurred_at))).all()
    assert stored == ["OrderCreated", "StockReserved", "PaymentApproved", "OrderConfirmed"]


async def test_topic_that_the_broker_refuses_to_create_is_reported_as_an_error(
    kafka: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(consumer_module, "PARTITIONS_PER_TOPIC", 0)

    with (
        caplog.at_level(logging.INFO, logger="order_analytics.consumer"),
        pytest.raises(InvalidPartitionsError, match=r"\.events"),
    ):
        await consumer_module._ensure_topics(kafka)

    assert "topics created" not in caplog.text


async def test_topic_that_someone_else_created_in_the_meantime_is_not_an_error(
    kafka: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    await consumer_module._ensure_topics(kafka)

    async def nothing_yet(self: AIOKafkaAdminClient) -> list[str]:
        return []

    # The topics exist, but this instance listed them a moment before they were created.
    monkeypatch.setattr(AIOKafkaAdminClient, "list_topics", nothing_yet)
    with caplog.at_level(logging.INFO, logger="order_analytics.consumer"):
        await consumer_module._ensure_topics(kafka)

    assert "topics created" not in caplog.text

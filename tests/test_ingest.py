import asyncio
import json
import random

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics import metrics
from order_analytics.ingest import Outcome, ingest
from order_analytics.seed import (
    ANA,
    HEADSET,
    KEYBOARD,
    MOUSE,
    NOTEBOOK,
    ORDERS,
    Ending,
    ExampleOrder,
    SeedMessage,
    story,
)
from order_analytics.tables import Event, Order, OrderItem, RejectedEvent
from tests.support import custom, raw, read_model, receive, received

Sessions = async_sessionmaker[AsyncSession]

CONFIRMED = ExampleOrder(
    "2026-10-05T09:15:00", ANA, ((KEYBOARD, 2), (MOUSE, 1)), Ending.CONFIRMED, 4
)
OUT_OF_STOCK = ExampleOrder("2026-10-05T10:00:00", ANA, ((HEADSET, 1),), Ending.OUT_OF_STOCK)
REFUSED = ExampleOrder("2026-10-05T11:00:00", ANA, ((NOTEBOOK, 1),), Ending.PAYMENT_REFUSED)


def three_orders() -> list[SeedMessage]:
    return [
        *story("order-1", CONFIRMED),
        *story("order-2", OUT_OF_STOCK),
        *story("order-3", REFUSED),
    ]


async def count(sessions: Sessions, table: type) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(table)) or 0


async def summary(sessions: Sessions) -> metrics.Summary:
    async with sessions() as session:
        return await metrics.summary(session)


async def test_stores_the_event_and_keeps_the_order_up_to_date(sessions: Sessions) -> None:
    outcomes = await receive(sessions, story("order-1", CONFIRMED))

    assert outcomes == [Outcome.STORED] * 4
    async with sessions() as session:
        order = await session.get_one(Order, "order-1")
        items = (await session.scalars(select(OrderItem).order_by(OrderItem.position))).all()
        kinds = (await session.scalars(select(Event.type).order_by(Event.occurred_at))).all()
    assert order.status == "CONFIRMED"
    assert order.customer_name == "Ana Souza"
    assert str(order.total_amount) == "829.70"
    assert [(item.product_name, item.quantity) for item in items] == [
        ("Teclado Mecânico", 2),
        ("Mouse Sem Fio", 1),
    ]
    assert kinds == ["OrderCreated", "StockReserved", "PaymentApproved", "OrderConfirmed"]


async def test_same_event_twice_changes_nothing(sessions: Sessions) -> None:
    messages = three_orders()
    await receive(sessions, messages)
    before = (await summary(sessions), await read_model(sessions), await count(sessions, Event))

    outcomes = await receive(sessions, messages)

    assert outcomes == [Outcome.DUPLICATE] * len(messages)
    assert (
        await summary(sessions),
        await read_model(sessions),
        await count(sessions, Event),
    ) == before


async def test_any_arrival_order_ends_in_the_same_read_model(sessions: Sessions) -> None:
    messages = three_orders()
    await receive(sessions, messages)
    expected = await read_model(sessions)
    assert len(expected["orders"]) == 3

    shuffles = [list(reversed(messages))]
    for seed in range(15):
        shuffled = list(messages)
        random.Random(seed).shuffle(shuffled)
        shuffles.append(shuffled)

    for arrival in shuffles:
        async with sessions.begin() as session:
            await session.execute(Event.__table__.delete())
            await session.execute(Order.__table__.delete())
        await receive(sessions, arrival)
        assert await read_model(sessions) == expected, [m.event_type for m in arrival]


async def test_order_is_visible_even_while_its_first_event_has_not_arrived(
    sessions: Sessions,
) -> None:
    created, reserved, _, confirmed = story("order-1", CONFIRMED)

    await receive(sessions, [confirmed])
    async with sessions() as session:
        order = await session.get_one(Order, "order-1")
    assert (order.status, order.created_at) == ("CONFIRMED", None)
    assert (await summary(sessions)).average_seconds_to_confirm is None

    await receive(sessions, [reserved, created])
    assert (await summary(sessions)).average_seconds_to_confirm == 4


async def test_events_of_the_same_order_stored_at_the_same_time_are_all_taken_into_account(
    sessions: Sessions,
) -> None:
    orders = {f"order-{number}": order for number, order in enumerate(ORDERS)}
    messages = [message for order_id, order in orders.items() for message in story(order_id, order)]
    await receive(sessions, messages)
    expected = await read_model(sessions)
    async with sessions.begin() as session:
        await session.execute(Event.__table__.delete())
        await session.execute(Order.__table__.delete())

    await asyncio.gather(*(ingest(sessions, received(message)) for message in messages))

    assert await read_model(sessions) == expected


async def test_invalid_message_is_set_aside_and_the_next_one_is_stored(sessions: Sessions) -> None:
    created = story("order-1", CONFIRMED)[0]
    broken_payload = custom("OrderCreated", "order-9", "2026-10-05T09:00:00Z", {"items": "none"})

    assert await ingest(sessions, raw(b"this is not json")) is Outcome.REJECTED
    assert await ingest(sessions, raw(b'{"type": "OrderCreated"}')) is Outcome.REJECTED
    assert await ingest(sessions, raw(None)) is Outcome.REJECTED
    assert await receive(sessions, [broken_payload, created]) == [Outcome.REJECTED, Outcome.STORED]

    async with sessions() as session:
        rejected = (await session.scalars(select(RejectedEvent).order_by(RejectedEvent.id))).all()
    assert [(each.message_value, each.reason.split(":")[0]) for each in rejected] == [
        ("this is not json", "invalid envelope"),
        ('{"type": "OrderCreated"}', "invalid envelope"),
        (None, "empty message"),
        (broken_payload.value.decode(), "invalid OrderCreated payload"),
    ]
    assert rejected[0].kafka_topic == "orders.events"
    assert await count(sessions, Event) == 1
    assert (await summary(sessions)).total_orders == 1


async def test_text_the_database_cannot_hold_is_rejected_instead_of_retried_forever(
    sessions: Sessions,
) -> None:
    with_nul = custom(
        "OrderCancelled", "order-1", "2026-10-05T09:00:00Z", {"reason": "OUT_OF\u0000STOCK"}
    )

    assert await receive(sessions, [with_nul]) == [Outcome.REJECTED]

    async with sessions() as session:
        rejected = (await session.scalars(select(RejectedEvent))).one()
    assert rejected.reason.startswith("refused by the database")
    assert await count(sessions, Event) == 0
    assert await count(sessions, Order) == 0


async def test_rejected_message_delivered_again_is_recorded_once(sessions: Sessions) -> None:
    message = raw(b"garbage")

    await ingest(sessions, message)
    await ingest(sessions, message)

    assert await count(sessions, RejectedEvent) == 1


async def test_unknown_event_type_is_stored_but_does_not_count_as_an_order(
    sessions: Sessions,
) -> None:
    shipped = custom("OrderShipped", "order-7", "2026-10-06T08:00:00Z", {"carrier": "Correios"})

    assert await receive(sessions, [shipped]) == [Outcome.STORED]

    assert await count(sessions, Event) == 1
    assert await count(sessions, Order) == 0
    result = await summary(sessions)
    assert result.total_orders == 0
    assert result.last_event_at is not None


async def test_payload_is_kept_as_it_came(sessions: Sessions) -> None:
    confirmed = story("order-1", CONFIRMED)[-1]

    await receive(sessions, [confirmed])

    async with sessions() as session:
        stored = (await session.scalars(select(Event))).one()
    assert stored.payload == json.loads(confirmed.value)["payload"]
    assert (stored.kafka_topic, stored.kafka_partition) == ("orders.events", 0)

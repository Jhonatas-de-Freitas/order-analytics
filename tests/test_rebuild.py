import asyncio
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics import metrics
from order_analytics.ingest import ingest
from order_analytics.rebuild import rebuild
from order_analytics.seed import ORDERS, SeedMessage, story
from tests.support import read_model, receive, received

Sessions = async_sessionmaker[AsyncSession]


def example_messages() -> list[SeedMessage]:
    return [
        message
        for number, order in enumerate(ORDERS)
        for message in story(f"order-{number}", order)
    ]


async def numbers(sessions: Sessions) -> list[Any]:
    async with sessions() as session:
        return [
            await metrics.summary(session),
            await metrics.orders_by_status(session),
            await metrics.revenue(session, None, None, metrics.Granularity.HOUR),
            await metrics.cancellations(session),
            await metrics.top_products(session, 10),
        ]


async def run_rebuild(sessions: Sessions) -> int:
    async with sessions.begin() as session:
        return await rebuild(session)


async def test_rebuild_gives_back_the_same_rows_and_the_same_numbers(sessions: Sessions) -> None:
    await receive(sessions, example_messages())
    before = (await read_model(sessions), await numbers(sessions))

    rebuilt = await run_rebuild(sessions)

    assert rebuilt == len(ORDERS)
    assert (await read_model(sessions), await numbers(sessions)) == before


async def test_rebuild_repairs_a_read_model_that_was_tampered_with(sessions: Sessions) -> None:
    await receive(sessions, example_messages())
    before = (await read_model(sessions), await numbers(sessions))
    async with sessions.begin() as session:
        await session.execute(text("UPDATE orders SET status = 'PENDING', total_amount = 1"))
        await session.execute(text("DELETE FROM order_items WHERE position = 0"))
        await session.execute(text("DELETE FROM orders WHERE order_id = 'order-3'"))
        await session.execute(
            text("INSERT INTO orders (order_id, status) VALUES ('ghost', 'CONFIRMED')")
        )

    await run_rebuild(sessions)

    assert (await read_model(sessions), await numbers(sessions)) == before


async def test_event_that_arrives_during_a_rebuild_waits_for_it_and_is_not_lost(
    sessions: Sessions,
) -> None:
    late = story("late-order", ORDERS[-1])
    await receive(sessions, example_messages())

    async with sessions() as rebuilding:
        await rebuild(rebuilding)
        arriving = asyncio.gather(*(ingest(sessions, received(message)) for message in late))
        await asyncio.sleep(0.5)
        assert not arriving.done()
        await rebuilding.commit()
    await arriving

    orders = (await read_model(sessions))["orders"]
    assert len(orders) == len(ORDERS) + 1
    assert "late-order" in [order["order_id"] for order in orders]

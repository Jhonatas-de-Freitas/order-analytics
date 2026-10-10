from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.ingest import Outcome
from order_analytics.seed import seed_messages
from tests.support import receive

Sessions = async_sessionmaker[AsyncSession]


async def test_seed_gives_the_numbers_that_the_readme_and_the_smoke_test_expect(
    sessions: Sessions, client: AsyncClient
) -> None:
    outcomes = await receive(sessions, seed_messages())

    assert outcomes.count(Outcome.DUPLICATE) == 1
    assert outcomes.count(Outcome.REJECTED) == 1
    assert (await client.get("/analytics/summary")).json() == {
        "totalOrders": 16,
        "confirmed": 9,
        "cancelled": 5,
        "inProgress": 2,
        "confirmedRevenue": 7871.9,
        "approvalRate": 0.6429,
        "averageSecondsToConfirm": 6.0,
        "lastEventAt": "2026-10-09T09:31:00Z",
    }
    assert (await client.get("/analytics/cancellations")).json() == [
        {"reason": "OUT_OF_STOCK", "count": 3},
        {"reason": "PAYMENT_REFUSED", "count": 2},
    ]
    assert (await client.get("/analytics/revenue")).json() == [
        {"bucket": "2026-10-05", "revenue": 1018.7, "orders": 2},
        {"bucket": "2026-10-06", "revenue": 2328.9, "orders": 2},
        {"bucket": "2026-10-07", "revenue": 3158.6, "orders": 2},
        {"bucket": "2026-10-08", "revenue": 1105.9, "orders": 2},
        {"bucket": "2026-10-09", "revenue": 259.8, "orders": 1},
    ]
    top = (await client.get("/analytics/top-products", params={"limit": 4})).json()
    assert [
        (product["productName"], product["quantity"], product["revenue"]) for product in top
    ] == [
        ("Mouse Sem Fio", 6, 779.4),
        ("Teclado Mecânico", 5, 1749.5),
        ("Webcam Full HD", 5, 945.0),
        ('Monitor 27" 4K', 2, 4398.0),
    ]


async def test_seed_published_twice_gives_the_same_numbers(
    sessions: Sessions, client: AsyncClient
) -> None:
    await receive(sessions, seed_messages())
    first = (await client.get("/analytics/summary")).json()

    outcomes = await receive(sessions, seed_messages())

    assert Outcome.STORED not in outcomes
    assert (await client.get("/analytics/summary")).json() == first

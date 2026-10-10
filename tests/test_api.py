import csv
import io
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.api import CSV_HEADER
from order_analytics.config import Settings
from order_analytics.main import create_app
from order_analytics.seed import (
    ANA,
    HEADSET,
    JOAO,
    KEYBOARD,
    MOUSE,
    NOTEBOOK,
    Ending,
    ExampleOrder,
    story,
)
from tests.support import receive

Sessions = async_sessionmaker[AsyncSession]

# Three confirmed (699.80 + 479.80 + 389.70), three cancelled, two still open.
# order-c is created at the end of day 2 and confirmed in the first seconds of day 3.
SHOP = {
    "order-a": ExampleOrder("2026-03-01T10:00:00", ANA, ((KEYBOARD, 2),), Ending.CONFIRMED, 4),
    "order-b": ExampleOrder(
        "2026-03-01T10:30:00", JOAO, ((MOUSE, 1), (KEYBOARD, 1)), Ending.CONFIRMED, 6
    ),
    "order-c": ExampleOrder("2026-03-02T23:59:58", ANA, ((MOUSE, 3),), Ending.CONFIRMED, 4),
    "order-d": ExampleOrder("2026-03-02T09:00:00", JOAO, ((HEADSET, 1),), Ending.OUT_OF_STOCK),
    "order-e": ExampleOrder("2026-03-02T15:00:00", ANA, ((NOTEBOOK, 1),), Ending.PAYMENT_REFUSED),
    "order-f": ExampleOrder(
        "2026-03-03T08:00:00", ANA, ((HEADSET, 2), (MOUSE, 1)), Ending.OUT_OF_STOCK
    ),
    "order-g": ExampleOrder("2026-03-03T12:00:00", ANA, ((KEYBOARD, 1),), Ending.JUST_CREATED),
    "order-h": ExampleOrder("2026-03-03T12:05:00", JOAO, ((MOUSE, 1),), Ending.WAITING_PAYMENT),
}

DAY_1 = {"bucket": "2026-03-01", "revenue": 1179.6, "orders": 2}
DAY_3 = {"bucket": "2026-03-03", "revenue": 389.7, "orders": 1}


@pytest.fixture
async def shop(sessions: Sessions) -> None:
    for order_id, order in SHOP.items():
        await receive(sessions, story(order_id, order))


@pytest.mark.usefixtures("shop")
async def test_summary(client: AsyncClient) -> None:
    response = await client.get("/analytics/summary")

    assert response.status_code == 200
    assert response.json() == {
        "totalOrders": 8,
        "confirmed": 3,
        "cancelled": 3,
        "inProgress": 2,
        "confirmedRevenue": 1569.3,
        "approvalRate": 0.5,
        "averageSecondsToConfirm": 4.67,
        "lastEventAt": "2026-03-03T12:05:01Z",
    }


async def test_summary_before_any_event(client: AsyncClient) -> None:
    response = await client.get("/analytics/summary")

    assert response.json() == {
        "totalOrders": 0,
        "confirmed": 0,
        "cancelled": 0,
        "inProgress": 0,
        "confirmedRevenue": 0,
        "approvalRate": 0,
        "averageSecondsToConfirm": None,
        "lastEventAt": None,
    }


@pytest.mark.usefixtures("shop")
async def test_orders_by_status_lists_the_four_statuses(client: AsyncClient) -> None:
    response = await client.get("/analytics/orders-by-status")

    assert response.json() == [
        {"status": "PENDING", "count": 1},
        {"status": "STOCK_RESERVED", "count": 1},
        {"status": "CONFIRMED", "count": 3},
        {"status": "CANCELLED", "count": 3},
    ]


@pytest.mark.usefixtures("shop")
async def test_revenue_by_day_uses_the_day_of_the_confirmation(client: AsyncClient) -> None:
    response = await client.get("/analytics/revenue")

    assert response.json() == [DAY_1, DAY_3]


@pytest.mark.usefixtures("shop")
async def test_revenue_by_hour(client: AsyncClient) -> None:
    response = await client.get("/analytics/revenue", params={"granularity": "hour"})

    assert response.json() == [
        {"bucket": "2026-03-01T10:00:00Z", "revenue": 1179.6, "orders": 2},
        {"bucket": "2026-03-03T00:00:00Z", "revenue": 389.7, "orders": 1},
    ]


@pytest.mark.usefixtures("shop")
@pytest.mark.parametrize(
    ("interval", "expected"),
    [
        ({"from": "2026-03-01", "to": "2026-03-03"}, [DAY_1, DAY_3]),
        ({"from": "2026-03-01", "to": "2026-03-01"}, [DAY_1]),
        ({"from": "2026-03-02", "to": "2026-03-02"}, []),
        ({"from": "2026-03-02"}, [DAY_3]),
        ({"to": "2026-03-02"}, [DAY_1]),
        ({"from": "0001-01-01", "to": "9999-12-31"}, [DAY_1, DAY_3]),
    ],
)
async def test_revenue_within_an_interval_that_includes_both_ends(
    client: AsyncClient, interval: dict[str, str], expected: list[dict[str, object]]
) -> None:
    response = await client.get("/analytics/revenue", params=interval)

    assert response.json() == expected


@pytest.mark.parametrize(
    "params",
    [
        {"from": "2026-03-03", "to": "2026-03-01"},
        {"from": "03/01/2026"},
        {"to": "2026-02-30"},
        {"granularity": "week"},
    ],
)
async def test_revenue_refuses_invalid_parameters(
    client: AsyncClient, params: dict[str, str]
) -> None:
    response = await client.get("/analytics/revenue", params=params)

    assert response.status_code == 422
    assert response.json()["detail"]


@pytest.mark.usefixtures("shop")
async def test_cancellations_by_reason(client: AsyncClient) -> None:
    response = await client.get("/analytics/cancellations")

    assert response.json() == [
        {"reason": "OUT_OF_STOCK", "count": 2},
        {"reason": "PAYMENT_REFUSED", "count": 1},
    ]


@pytest.mark.usefixtures("shop")
async def test_top_products_counts_only_confirmed_orders(client: AsyncClient) -> None:
    response = await client.get("/analytics/top-products")

    assert response.json() == [
        {"productId": MOUSE.id, "productName": "Mouse Sem Fio", "quantity": 4, "revenue": 519.6},
        {
            "productId": KEYBOARD.id,
            "productName": "Teclado Mecânico",
            "quantity": 3,
            "revenue": 1049.7,
        },
    ]


@pytest.mark.usefixtures("shop")
async def test_top_products_respects_the_limit(client: AsyncClient) -> None:
    response = await client.get("/analytics/top-products", params={"limit": 1})

    assert [product["productName"] for product in response.json()] == ["Mouse Sem Fio"]


@pytest.mark.parametrize("limit", ["0", "101", "many"])
async def test_top_products_refuses_an_invalid_limit(client: AsyncClient, limit: str) -> None:
    response = await client.get("/analytics/top-products", params={"limit": limit})

    assert response.status_code == 422


@pytest.mark.usefixtures("shop")
async def test_orders_report_is_a_csv_with_one_order_per_line(client: AsyncClient) -> None:
    response = await client.get("/analytics/reports/orders.csv")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "João Araújo".encode() in response.content

    header, *lines = csv.reader(io.StringIO(response.content.decode("utf-8")))
    assert tuple(header) == CSV_HEADER
    orders = {line[0]: dict(zip(header, line, strict=True)) for line in lines}
    assert list(orders) == [
        "order-a",
        "order-b",
        "order-d",
        "order-e",
        "order-c",
        "order-f",
        "order-g",
        "order-h",
    ]
    assert orders["order-a"] == {
        "orderId": "order-a",
        "status": "CONFIRMED",
        "customerId": "c-1001",
        "customerName": "Ana Souza",
        "customerEmail": "ana@exemplo.com",
        "totalAmount": "699.80",
        "itemCount": "1",
        "cancellationReason": "",
        "cancellationDetail": "",
        "createdAt": "2026-03-01T10:00:00Z",
        "finishedAt": "2026-03-01T10:00:04Z",
    }
    assert orders["order-b"]["customerName"] == "João Araújo"
    assert orders["order-b"]["itemCount"] == "2"
    assert orders["order-e"]["totalAmount"] == "7499.00"
    assert orders["order-f"]["cancellationReason"] == "OUT_OF_STOCK"
    assert orders["order-f"]["cancellationDetail"] == "Headset USB: pedido 2, disponível 0"
    assert orders["order-g"]["totalAmount"] == ""
    assert orders["order-g"]["finishedAt"] == ""


async def test_health_without_the_consumer(client: AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "database": "UP", "consumer": "DISABLED"}


@pytest.fixture
async def broken_client(database_url: str) -> AsyncIterator[AsyncClient]:
    """The API with a consumer that has no broker to talk to (nothing listens on port 1)."""
    app = create_app(Settings(database_url=database_url, kafka_bootstrap_servers="127.0.0.1:1"))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app), base_url="http://test") as http,
    ):
        yield http


async def test_health_is_down_while_the_consumer_is_not_connected(
    broken_client: AsyncClient,
) -> None:
    response = await broken_client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "DOWN", "database": "UP", "consumer": "DOWN"}


async def test_health_is_down_without_the_database() -> None:
    app = create_app(
        Settings(database_url="postgresql://nobody@127.0.0.1:1/nothing", kafka_enabled=False)
    )
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app), base_url="http://test") as http,
    ):
        response = await http.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "DOWN", "database": "DOWN", "consumer": "DISABLED"}


async def test_docs_describe_the_endpoints_of_the_contract_with_examples(
    client: AsyncClient,
) -> None:
    assert (await client.get("/docs")).status_code == 200

    openapi = (await client.get("/openapi.json")).json()
    assert set(openapi["paths"]) == {
        "/analytics/summary",
        "/analytics/orders-by-status",
        "/analytics/revenue",
        "/analytics/cancellations",
        "/analytics/top-products",
        "/analytics/reports/orders.csv",
        "/health",
    }
    for schema in ("Summary", "StatusCount", "RevenueBucket", "CancellationCount", "ProductSales"):
        assert openapi["components"]["schemas"][schema]["examples"]
    revenue = {p["name"] for p in openapi["paths"]["/analytics/revenue"]["get"]["parameters"]}
    assert revenue == {"from", "to", "granularity"}

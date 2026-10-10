import csv
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from typing import Annotated, Literal, Self

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics import metrics
from order_analytics.consumer import EventConsumer
from order_analytics.tables import Order, OrderItem

router = APIRouter()


def get_sessions(request: Request) -> async_sessionmaker[AsyncSession]:
    sessions: async_sessionmaker[AsyncSession] = request.app.state.sessions
    return sessions


Sessions = Annotated[async_sessionmaker[AsyncSession], Depends(get_sessions)]


async def get_session(sessions: Sessions) -> AsyncIterator[AsyncSession]:
    async with sessions() as session:
        yield session


Session = Annotated[AsyncSession, Depends(get_session)]


class RevenueParams(BaseModel):
    from_: date | None = Field(
        None, alias="from", description="First day, inclusive (UTC).", examples=["2026-10-05"]
    )
    to: date | None = Field(None, description="Last day, inclusive (UTC).", examples=["2026-10-09"])
    granularity: metrics.Granularity = metrics.Granularity.DAY

    @model_validator(mode="after")
    def from_not_after_to(self) -> Self:
        if self.from_ and self.to and self.from_ > self.to:
            raise ValueError("'from' must not be after 'to'")
        return self


@router.get("/analytics/summary", tags=["analytics"])
async def summary(session: Session) -> metrics.Summary:
    """Totals for everything received so far.

    `approvalRate` is confirmed / (confirmed + cancelled), and 0 while no order has finished.
    `averageSecondsToConfirm` is the mean time between OrderCreated and OrderConfirmed.
    """
    return await metrics.summary(session)


@router.get("/analytics/orders-by-status", tags=["analytics"])
async def orders_by_status(session: Session) -> list[metrics.StatusCount]:
    """How many orders are in each status. All four statuses are always listed."""
    return await metrics.orders_by_status(session)


@router.get("/analytics/revenue", tags=["analytics"])
async def revenue(
    session: Session, params: Annotated[RevenueParams, Query()]
) -> list[metrics.RevenueBucket]:
    """Revenue of confirmed orders by day or by hour (UTC), oldest first.

    A bucket is the moment the order was confirmed. Days or hours without a confirmed
    order are left out.
    """
    return await metrics.revenue(session, params.from_, params.to, params.granularity)


@router.get("/analytics/cancellations", tags=["analytics"])
async def cancellations(session: Session) -> list[metrics.CancellationCount]:
    """Cancelled orders by reason, most frequent first."""
    return await metrics.cancellations(session)


@router.get("/analytics/top-products", tags=["analytics"])
async def top_products(
    session: Session, limit: Annotated[int, Query(ge=1, le=100)] = 5
) -> list[metrics.ProductSales]:
    """Products that sold the most units in confirmed orders."""
    return await metrics.top_products(session, limit)


CSV_HEADER = (
    "orderId",
    "status",
    "customerId",
    "customerName",
    "customerEmail",
    "totalAmount",
    "itemCount",
    "cancellationReason",
    "cancellationDetail",
    "createdAt",
    "finishedAt",
)


class _Line:
    """File-like object for csv.writer that hands each formatted line back to the caller."""

    def write(self, line: str) -> str:
        return line


def _timestamp(moment: datetime | None) -> str | None:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z") if moment else None


async def _orders_csv(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[str]:
    item_count = (
        select(func.count())
        .where(OrderItem.order_id == Order.order_id)
        .correlate(Order)
        .scalar_subquery()
    )
    query = select(Order, item_count).order_by(Order.created_at.nulls_last(), Order.order_id)

    writer = csv.writer(_Line())
    yield writer.writerow(CSV_HEADER)
    async with sessions() as session:
        # Server-side cursor: the report is sent as it is read, whatever its size.
        rows = await session.stream(query.execution_options(yield_per=500))
        async for order, items in rows:
            yield writer.writerow(
                (
                    order.order_id,
                    order.status,
                    order.customer_id,
                    order.customer_name,
                    order.customer_email,
                    order.total_amount,
                    items,
                    order.cancellation_reason,
                    order.cancellation_detail,
                    _timestamp(order.created_at),
                    _timestamp(order.finished_at),
                )
            )


@router.get(
    "/analytics/reports/orders.csv",
    tags=["analytics"],
    response_class=StreamingResponse,
    responses={200: {"content": {"text/csv": {}}, "description": "One order per line."}},
)
async def orders_report(sessions: Sessions) -> StreamingResponse:
    """Every order as CSV (UTF-8, comma separated, dot as decimal mark), oldest first."""
    return StreamingResponse(
        _orders_csv(sessions),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="orders.csv"'},
    )


class Health(BaseModel):
    status: Literal["UP", "DOWN"]
    database: Literal["UP", "DOWN"]
    consumer: Literal["UP", "DOWN", "DISABLED"]


@router.get("/health", tags=["health"], responses={503: {"model": Health}})
async def health(request: Request, sessions: Sessions, response: Response) -> Health:
    """UP when the database answers and the Kafka consumer is running."""
    consumer: EventConsumer | None = request.app.state.consumer
    try:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
        database_up = True
    except (SQLAlchemyError, OSError):
        database_up = False

    healthy = database_up and (consumer is None or consumer.running)
    if not healthy:
        response.status_code = 503
    return Health(
        status="UP" if healthy else "DOWN",
        database="UP" if database_up else "DOWN",
        consumer="DISABLED" if consumer is None else "UP" if consumer.running else "DOWN",
    )

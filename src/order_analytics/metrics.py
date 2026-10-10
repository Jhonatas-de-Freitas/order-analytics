from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer
from pydantic.alias_generators import to_camel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from order_analytics.projection import OrderStatus
from order_analytics.tables import Event, Order, OrderItem

# Money is exact in the database and in the sums; it only becomes a float on the way out,
# because the contract asks for JSON numbers.
Amount = Annotated[Decimal, PlainSerializer(float, return_type=float, when_used="json")]

_CONFIRMED = Order.status == OrderStatus.CONFIRMED
_CANCELLED = Order.status == OrderStatus.CANCELLED


class Granularity(StrEnum):
    DAY = "day"
    HOUR = "hour"


class _Metric(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class Summary(_Metric):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "totalOrders": 16,
                    "confirmed": 9,
                    "cancelled": 5,
                    "inProgress": 2,
                    "confirmedRevenue": 7871.9,
                    "approvalRate": 0.6429,
                    "averageSecondsToConfirm": 6.0,
                    "lastEventAt": "2026-10-09T09:31:00Z",
                }
            ]
        }
    )

    total_orders: int
    confirmed: int
    cancelled: int
    in_progress: int
    confirmed_revenue: Amount
    approval_rate: float
    average_seconds_to_confirm: float | None
    last_event_at: datetime | None


class StatusCount(_Metric):
    model_config = ConfigDict(json_schema_extra={"examples": [{"status": "CONFIRMED", "count": 9}]})

    status: OrderStatus
    count: int


class RevenueBucket(_Metric):
    model_config = ConfigDict(
        json_schema_extra={"examples": [{"bucket": "2026-10-05", "revenue": 1018.7, "orders": 2}]}
    )

    bucket: str
    revenue: Amount
    orders: int


class CancellationCount(_Metric):
    model_config = ConfigDict(
        json_schema_extra={"examples": [{"reason": "OUT_OF_STOCK", "count": 3}]}
    )

    reason: str
    count: int


class ProductSales(_Metric):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "productId": "22222222-2222-2222-2222-222222222222",
                    "productName": "Mouse Sem Fio",
                    "quantity": 6,
                    "revenue": 779.4,
                }
            ]
        }
    )

    product_id: str
    product_name: str | None
    quantity: int
    revenue: Amount


async def summary(session: AsyncSession) -> Summary:
    row = (
        await session.execute(
            select(
                func.count().label("total"),
                func.count().filter(_CONFIRMED).label("confirmed"),
                func.count().filter(_CANCELLED).label("cancelled"),
                func.coalesce(func.sum(Order.total_amount).filter(_CONFIRMED), 0).label("revenue"),
                func.avg(func.extract("epoch", Order.finished_at - Order.created_at))
                .filter(_CONFIRMED)
                .label("seconds_to_confirm"),
            ).select_from(Order)
        )
    ).one()
    finished = row.confirmed + row.cancelled
    return Summary(
        total_orders=row.total,
        confirmed=row.confirmed,
        cancelled=row.cancelled,
        in_progress=row.total - finished,
        confirmed_revenue=row.revenue,
        approval_rate=round(row.confirmed / finished, 4) if finished else 0,
        average_seconds_to_confirm=(
            None if row.seconds_to_confirm is None else round(float(row.seconds_to_confirm), 2)
        ),
        last_event_at=await session.scalar(select(func.max(Event.occurred_at))),
    )


async def orders_by_status(session: AsyncSession) -> list[StatusCount]:
    rows = await session.execute(select(Order.status, func.count()).group_by(Order.status))
    counts = dict(rows.all())
    return [StatusCount(status=status, count=counts.get(status, 0)) for status in OrderStatus]


async def revenue(
    session: AsyncSession, start: date | None, end: date | None, granularity: Granularity
) -> list[RevenueBucket]:
    """Revenue of confirmed orders, by the UTC day or hour in which they were confirmed."""
    bucket = func.date_trunc(granularity, func.timezone("UTC", Order.finished_at)).label("bucket")
    query = (
        select(bucket, func.sum(Order.total_amount), func.count())
        .where(_CONFIRMED)
        .group_by(bucket)
        .order_by(bucket)
    )
    if start:
        query = query.where(Order.finished_at >= datetime.combine(start, time.min, UTC))
    if end:
        query = query.where(Order.finished_at <= datetime.combine(end, time.max, UTC))

    label = "%Y-%m-%d" if granularity is Granularity.DAY else "%Y-%m-%dT%H:00:00Z"
    return [
        RevenueBucket(bucket=moment.strftime(label), revenue=amount, orders=orders)
        for moment, amount, orders in await session.execute(query)
    ]


async def cancellations(session: AsyncSession) -> list[CancellationCount]:
    count = func.count()
    rows = await session.execute(
        select(Order.cancellation_reason, count)
        .where(_CANCELLED)
        .group_by(Order.cancellation_reason)
        .order_by(count.desc(), Order.cancellation_reason)
    )
    return [CancellationCount(reason=reason, count=total) for reason, total in rows]


async def top_products(session: AsyncSession, limit: int) -> list[ProductSales]:
    """Best sellers by units, counting only orders that were confirmed."""
    quantity = func.sum(OrderItem.quantity)
    amount = func.coalesce(func.sum(OrderItem.quantity * OrderItem.unit_price), 0)
    rows = await session.execute(
        select(OrderItem.product_id, func.max(OrderItem.product_name), quantity, amount)
        .join(Order, Order.order_id == OrderItem.order_id)
        .where(_CONFIRMED)
        .group_by(OrderItem.product_id)
        .order_by(quantity.desc(), amount.desc(), OrderItem.product_id)
        .limit(limit)
    )
    return [
        ProductSales(product_id=product_id, product_name=name, quantity=units, revenue=total)
        for product_id, name, units, total in rows
    ]

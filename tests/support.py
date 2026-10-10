import itertools
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.events import parse_event
from order_analytics.ingest import Message, Outcome, ingest
from order_analytics.seed import SeedMessage
from order_analytics.tables import Event, Order, OrderItem

_offsets = itertools.count()


def raw(value: bytes | None, topic: str = "orders.events") -> Message:
    """Arbitrary bytes as if they had just been read from Kafka."""
    return Message(topic=topic, partition=0, offset=next(_offsets), key=b"key", value=value)


def received(message: SeedMessage) -> Message:
    return Message(
        topic=message.topic,
        partition=0,
        offset=next(_offsets),
        key=message.key,
        value=message.value,
    )


def custom(
    event_type: str, order_id: str, occurred_at: str, payload: dict[str, Any]
) -> SeedMessage:
    """An event outside the usual stories, for the cases the seed builder does not produce."""
    envelope = {
        "eventId": f"{order_id}/{event_type}/{occurred_at}",
        "type": event_type,
        "orderId": order_id,
        "occurredAt": occurred_at,
        "payload": payload,
    }
    return SeedMessage(
        "orders.events", order_id.encode(), json.dumps(envelope).encode(), event_type
    )


def as_event(message: SeedMessage) -> Event:
    """The row that storing this message would create, without touching a database."""
    envelope = parse_event(message.value)
    return Event(
        event_id=envelope.event_id,
        type=envelope.type,
        order_id=envelope.order_id,
        occurred_at=envelope.occurred_at,
        payload=envelope.payload,
    )


async def receive(
    sessions: async_sessionmaker[AsyncSession], messages: list[SeedMessage]
) -> list[Outcome]:
    return [await ingest(sessions, received(message)) for message in messages]


async def read_model(sessions: async_sessionmaker[AsyncSession]) -> dict[str, Any]:
    """Everything in `orders` and `order_items`, in a form that two runs can be compared by."""
    async with sessions() as session:
        orders = (await session.scalars(select(Order).order_by(Order.order_id))).all()
        items = (
            await session.scalars(
                select(OrderItem).order_by(OrderItem.order_id, OrderItem.position)
            )
        ).all()
    return {
        "orders": [_columns(order) for order in orders],
        "items": [_columns(item) for item in items],
    }


def _columns(row: Order | OrderItem) -> dict[str, Any]:
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel

from order_analytics.events import (
    Item,
    OrderCancelled,
    OrderConfirmed,
    OrderCreated,
    StockReserved,
    WithCustomer,
)
from order_analytics.tables import Event


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    STOCK_RESERVED = "STOCK_RESERVED"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class OrderState:
    status: OrderStatus
    customer_id: str | None
    customer_name: str | None
    customer_email: str | None
    total_amount: Decimal | None
    cancellation_reason: str | None
    cancellation_detail: str | None
    created_at: datetime | None
    finished_at: datetime | None
    items: tuple[Item, ...]


def project(events: Iterable[Event]) -> OrderState | None:
    """Work out the state of one order from the events known about it.

    The answer depends only on which events are given, never on the order they come in:
    each field is read from one specific event, and the choice of that event uses the
    time the event happened, not the time it arrived. That is what lets the consumer
    receive OrderConfirmed before OrderCreated and still end up with the same row.

    Returns None when no event says anything about the order itself.
    """
    earliest: dict[str, Event] = {}
    for event in sorted(events, key=_when):
        earliest.setdefault(event.type, event)

    created = _read(OrderCreated, earliest.get("OrderCreated"))
    reserved = _read(StockReserved, earliest.get("StockReserved"))
    confirmed = _read(OrderConfirmed, earliest.get("OrderConfirmed"))
    cancelled = _read(OrderCancelled, earliest.get("OrderCancelled"))
    if not (created or reserved or confirmed or cancelled):
        return None

    # CONFIRMED and CANCELLED are final in the order-service: whichever happened first stands.
    final = min(
        (e for e in (earliest.get("OrderConfirmed"), earliest.get("OrderCancelled")) if e),
        key=_when,
        default=None,
    )
    if final is None:
        status = OrderStatus.STOCK_RESERVED if reserved else OrderStatus.PENDING
    elif final.type == "OrderConfirmed":
        status = OrderStatus.CONFIRMED
    else:
        status = OrderStatus.CANCELLED
    was_cancelled = cancelled if status is OrderStatus.CANCELLED else None

    with_customer: list[WithCustomer] = [p for p in (created, confirmed, cancelled) if p]
    # Names, prices and the total only exist once the stock is reserved;
    # OrderCreated carries just product ids and quantities.
    priced = confirmed or reserved
    listed = priced or created

    return OrderState(
        status=status,
        customer_id=next((p.customer_id for p in with_customer if p.customer_id), None),
        customer_name=next((p.customer_name for p in with_customer if p.customer_name), None),
        customer_email=next((p.customer_email for p in with_customer if p.customer_email), None),
        total_amount=priced.total_amount if priced else None,
        cancellation_reason=was_cancelled.reason if was_cancelled else None,
        cancellation_detail=was_cancelled.detail if was_cancelled else None,
        created_at=earliest["OrderCreated"].occurred_at if created else None,
        finished_at=final.occurred_at if final else None,
        items=tuple(listed.items) if listed else (),
    )


def _when(event: Event) -> tuple[datetime, str]:
    # The id breaks ties so that two events with the same timestamp always sort the same way.
    return event.occurred_at, event.event_id


def _read[P: BaseModel](model: type[P], event: Event | None) -> P | None:
    return model.model_validate(event.payload) if event else None

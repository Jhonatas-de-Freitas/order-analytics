"""Publishes a fixed set of example events, so the API has numbers to show on its own.

    python -m order_analytics.seed

The set is always the same (same ids, same dates), so running it again only produces
duplicates, which the consumer ignores. The events are sent in a scrambled order on
purpose, together with one repeated event and one broken message.
"""

import asyncio
import json
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum, auto
from typing import Any

from aiokafka import AIOKafkaProducer

from order_analytics.config import Settings
from order_analytics.logs import configure_logging

# Not __name__: this module is run with `python -m`, where that would be "__main__".
logger = logging.getLogger("order_analytics.seed")

_NAMESPACE = uuid.UUID("6f0e3c52-8f0a-4d3b-9a57-2f1d5c0b7e10")

_TOPIC_OF = {
    "OrderCreated": "orders.events",
    "OrderConfirmed": "orders.events",
    "OrderCancelled": "orders.events",
    "StockReserved": "inventory.events",
    "StockRejected": "inventory.events",
    "StockReleased": "inventory.events",
    "PaymentApproved": "payments.events",
    "PaymentRefused": "payments.events",
}


@dataclass(frozen=True)
class Customer:
    id: str
    name: str
    email: str


@dataclass(frozen=True)
class Product:
    id: str
    name: str
    price: Decimal


class Ending(Enum):
    CONFIRMED = auto()
    OUT_OF_STOCK = auto()
    PAYMENT_REFUSED = auto()
    WAITING_PAYMENT = auto()
    JUST_CREATED = auto()


@dataclass(frozen=True)
class ExampleOrder:
    created_at: str
    customer: Customer
    items: tuple[tuple[Product, int], ...]
    ending: Ending
    seconds_to_confirm: int = 0


@dataclass(frozen=True)
class SeedMessage:
    topic: str
    key: bytes
    value: bytes
    event_type: str


ANA = Customer("c-1001", "Ana Souza", "ana@exemplo.com")
BRUNO = Customer("c-1002", "Bruno Lima", "bruno@exemplo.com")
CARLA = Customer("c-1003", "Carla Mendes", "carla@exemplo.com")
JOAO = Customer("c-1004", "João Araújo", "joao@exemplo.com")
LUCIA = Customer("c-1005", "Lúcia Conceição", "lucia@exemplo.com")

KEYBOARD = Product("11111111-1111-1111-1111-111111111111", "Teclado Mecânico", Decimal("349.90"))
MOUSE = Product("22222222-2222-2222-2222-222222222222", "Mouse Sem Fio", Decimal("129.90"))
MONITOR = Product("33333333-3333-3333-3333-333333333333", 'Monitor 27" 4K', Decimal("2199.00"))
HEADSET = Product("44444444-4444-4444-4444-444444444444", "Headset USB", Decimal("279.50"))
NOTEBOOK = Product("55555555-5555-5555-5555-555555555555", "Notebook Pro 16", Decimal("7499.00"))
WEBCAM = Product("66666666-6666-6666-6666-666666666666", "Webcam Full HD", Decimal("189.00"))

# Five days of a small shop: 9 confirmed, 3 without stock, 2 refused by payment, 2 still open.
ORDERS = (
    ExampleOrder("2026-10-05T09:15:00", ANA, ((KEYBOARD, 2),), Ending.CONFIRMED, 4),
    ExampleOrder("2026-10-05T09:40:10", BRUNO, ((MOUSE, 1), (WEBCAM, 1)), Ending.CONFIRMED, 6),
    ExampleOrder("2026-10-05T14:05:30", CARLA, ((HEADSET, 1),), Ending.OUT_OF_STOCK),
    ExampleOrder("2026-10-06T10:20:00", JOAO, ((MONITOR, 1),), Ending.CONFIRMED, 5),
    ExampleOrder("2026-10-06T10:45:00", LUCIA, ((NOTEBOOK, 1),), Ending.PAYMENT_REFUSED),
    ExampleOrder("2026-10-06T16:30:00", ANA, ((MOUSE, 1),), Ending.CONFIRMED, 3),
    ExampleOrder("2026-10-06T17:10:00", BRUNO, ((HEADSET, 1), (MOUSE, 1)), Ending.OUT_OF_STOCK),
    ExampleOrder("2026-10-07T08:55:00", BRUNO, ((KEYBOARD, 1), (MOUSE, 2)), Ending.CONFIRMED, 8),
    ExampleOrder("2026-10-07T11:10:00", CARLA, ((MONITOR, 1), (KEYBOARD, 1)), Ending.CONFIRMED, 6),
    ExampleOrder("2026-10-07T11:25:00", JOAO, ((HEADSET, 2),), Ending.OUT_OF_STOCK),
    ExampleOrder("2026-10-08T13:00:00", LUCIA, ((WEBCAM, 3),), Ending.CONFIRMED, 4),
    ExampleOrder("2026-10-08T13:20:00", ANA, ((NOTEBOOK, 1), (MOUSE, 1)), Ending.PAYMENT_REFUSED),
    ExampleOrder("2026-10-08T19:45:00", BRUNO, ((WEBCAM, 1), (KEYBOARD, 1)), Ending.CONFIRMED, 6),
    ExampleOrder("2026-10-09T09:05:00", CARLA, ((MOUSE, 2),), Ending.CONFIRMED, 12),
    ExampleOrder("2026-10-09T09:30:00", JOAO, ((KEYBOARD, 1),), Ending.WAITING_PAYMENT),
    ExampleOrder("2026-10-09T09:31:00", LUCIA, ((WEBCAM, 1),), Ending.JUST_CREATED),
)


def seed_messages() -> list[SeedMessage]:
    messages = [
        message
        for number, order in enumerate(ORDERS, start=1)
        for message in story(str(uuid.uuid5(_NAMESPACE, f"order-{number}")), order)
    ]
    random.Random(2026).shuffle(messages)
    messages.append(messages[0])
    messages.append(
        SeedMessage("orders.events", b"broken", b'{"type": "OrderCreated"}', "OrderCreated")
    )
    return messages


def story(order_id: str, order: ExampleOrder) -> list[SeedMessage]:
    """The events the three services would publish for this order, in the order they happen."""
    created_at = datetime.fromisoformat(order.created_at).replace(tzinfo=UTC)
    total = sum((product.price * quantity for product, quantity in order.items), Decimal(0))

    customer = {
        "customerId": order.customer.id,
        "customerName": order.customer.name,
        "customerEmail": order.customer.email,
    }
    requested = [
        {"productId": product.id, "quantity": quantity} for product, quantity in order.items
    ]
    priced = [
        {
            "productId": product.id,
            "productName": product.name,
            "quantity": quantity,
            "unitPrice": float(product.price),
        }
        for product, quantity in order.items
    ]
    payment = {
        "paymentId": str(uuid.uuid5(_NAMESPACE, f"{order_id}/payment")),
        "amount": float(total),
    }

    def event(event_type: str, after_seconds: int, payload: dict[str, Any]) -> SeedMessage:
        occurred_at = created_at + timedelta(seconds=after_seconds)
        envelope = {
            "eventId": str(uuid.uuid5(_NAMESPACE, f"{order_id}/{event_type}")),
            "type": event_type,
            "orderId": order_id,
            "occurredAt": occurred_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "payload": payload,
        }
        return SeedMessage(
            topic=_TOPIC_OF[event_type],
            key=order_id.encode(),
            value=json.dumps(envelope, ensure_ascii=False).encode(),
            event_type=event_type,
        )

    events = [event("OrderCreated", 0, {**customer, "items": requested})]
    reserved = event("StockReserved", 1, {"items": priced, "totalAmount": float(total)})

    match order.ending:
        case Ending.CONFIRMED:
            events += [
                reserved,
                event("PaymentApproved", order.seconds_to_confirm - 1, payment),
                event(
                    "OrderConfirmed",
                    order.seconds_to_confirm,
                    {**customer, "totalAmount": float(total), "items": priced},
                ),
            ]
        case Ending.OUT_OF_STOCK:
            missing = [
                (product, quantity) for product, quantity in order.items if product is HEADSET
            ]
            events += [
                event(
                    "StockRejected",
                    1,
                    {
                        "reason": "INSUFFICIENT_STOCK",
                        "unavailableItems": [
                            {
                                "productId": product.id,
                                "requestedQuantity": quantity,
                                "availableQuantity": 0,
                            }
                            for product, quantity in missing
                        ],
                    },
                ),
                event(
                    "OrderCancelled",
                    2,
                    {
                        **customer,
                        "reason": "OUT_OF_STOCK",
                        "detail": "; ".join(
                            f"{product.name}: pedido {quantity}, disponível 0"
                            for product, quantity in missing
                        ),
                    },
                ),
            ]
        case Ending.PAYMENT_REFUSED:
            events += [
                reserved,
                event("PaymentRefused", 2, {**payment, "reason": "LIMIT_EXCEEDED"}),
                event(
                    "OrderCancelled",
                    3,
                    {**customer, "reason": "PAYMENT_REFUSED", "detail": "LIMIT_EXCEEDED"},
                ),
                event("StockReleased", 4, {"items": requested}),
            ]
        case Ending.WAITING_PAYMENT:
            events.append(reserved)
        case Ending.JUST_CREATED:
            pass
    return events


async def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    messages = seed_messages()

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        for message in messages:
            await producer.send_and_wait(
                message.topic,
                value=message.value,
                key=message.key,
                headers=[("event-type", message.event_type.encode())],
            )
    finally:
        await producer.stop()
    logger.info("seed published", extra={"messages": len(messages), "orders": len(ORDERS)})


if __name__ == "__main__":
    asyncio.run(main())

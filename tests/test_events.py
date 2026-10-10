import json
from datetime import UTC, datetime
from typing import Any

import pytest

from order_analytics.events import InvalidEventError, OrderConfirmed, parse_event

# The examples below are copied from the system contract, not built by this project's code.
ORDER_CONFIRMED = """
{
  "eventId": "0b9d6c1e-4a7b-4f0e-9a51-0c6f3d2a8e11",
  "type": "OrderConfirmed",
  "orderId": "7c1f0a34-5b1d-4f6e-8a2b-9d3e4f5a6b7c",
  "occurredAt": "2026-10-10T14:03:22.123Z",
  "payload": {
    "customerId": "c-1001", "customerName": "Ana Souza", "customerEmail": "ana@exemplo.com",
    "totalAmount": 699.80,
    "items": [ { "productId": "11111111-1111-1111-1111-111111111111",
                 "productName": "Teclado Mecânico", "quantity": 2, "unitPrice": 349.90 } ]
  }
}
"""

CONTRACT_PAYLOADS = {
    "OrderCreated": """
        { "customerId": "c-1001", "customerName": "Ana Souza", "customerEmail": "ana@exemplo.com",
          "items": [ { "productId": "11111111-1111-1111-1111-111111111111", "quantity": 2 } ] }""",
    "OrderCancelled": """
        { "customerId": "c-1001", "customerName": "Ana Souza", "customerEmail": "ana@exemplo.com",
          "reason": "OUT_OF_STOCK", "detail": "Headset USB: pedido 1, disponível 0" }""",
    "StockReserved": """
        { "items": [ { "productId": "11111111-1111-1111-1111-111111111111",
                       "productName": "Teclado Mecânico", "quantity": 2, "unitPrice": 349.90 } ],
          "totalAmount": 699.80 }""",
    "StockRejected": """
        { "reason": "INSUFFICIENT_STOCK",
          "unavailableItems": [ { "productId": "44444444-4444-4444-4444-444444444444",
                                  "requestedQuantity": 1, "availableQuantity": 0 } ] }""",
    "StockReleased": """
        { "items": [ { "productId": "55555555-5555-5555-5555-555555555555", "quantity": 1 } ] }""",
    "PaymentApproved": """{ "paymentId": "a3c1", "amount": 699.80 }""",
    "PaymentRefused": """{ "paymentId": "a3c1", "amount": 7499.00, "reason": "LIMIT_EXCEEDED" }""",
}


def envelope(**changes: Any) -> bytes:
    content = json.loads(ORDER_CONFIRMED) | changes
    return json.dumps({key: value for key, value in content.items() if value is not ...}).encode()


def test_reads_the_envelope_of_the_contract() -> None:
    event = parse_event(ORDER_CONFIRMED.encode())

    assert event.event_id == "0b9d6c1e-4a7b-4f0e-9a51-0c6f3d2a8e11"
    assert event.type == "OrderConfirmed"
    assert event.order_id == "7c1f0a34-5b1d-4f6e-8a2b-9d3e4f5a6b7c"
    assert event.occurred_at == datetime(2026, 10, 10, 14, 3, 22, 123000, tzinfo=UTC)
    assert OrderConfirmed.model_validate(event.payload).total_amount.as_tuple().exponent == -2


@pytest.mark.parametrize("event_type", CONTRACT_PAYLOADS)
def test_accepts_every_payload_of_the_contract(event_type: str) -> None:
    payload = json.loads(CONTRACT_PAYLOADS[event_type])

    assert parse_event(envelope(type=event_type, payload=payload)).payload == payload


def test_ignores_fields_it_does_not_know() -> None:
    payload = json.loads(ORDER_CONFIRMED)["payload"] | {"coupon": "BEMVINDO"}

    event = parse_event(envelope(payload=payload, traceId="abc"))

    assert event.payload["coupon"] == "BEMVINDO"


def test_keeps_an_event_of_unknown_type() -> None:
    event = parse_event(envelope(type="OrderShipped", payload={"carrier": "Correios"}))

    assert event.type == "OrderShipped"


def test_converts_the_date_to_utc() -> None:
    event = parse_event(envelope(occurredAt="2026-10-10T11:03:22-03:00"))

    assert event.occurred_at == datetime(2026, 10, 10, 14, 3, 22, tzinfo=UTC)
    assert event.occurred_at.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (None, "empty message"),
        (b"", "empty message"),
        (b"not json at all", "not valid JSON"),
        (b"\xff\xfe\x00", "not valid JSON"),
        (b'["a", "list"]', "invalid envelope"),
        (envelope(eventId=...), "eventId: Field required"),
        (envelope(orderId=""), "orderId"),
        (envelope(orderId=42), "orderId"),
        (envelope(occurredAt="yesterday"), "occurredAt"),
        (envelope(occurredAt="2026-10-10T14:03:22"), "occurredAt"),
        (envelope(occurredAt="9999-12-31T23:59:59-05:00"), "date out of range"),
        (envelope(payload="text"), "payload"),
        (envelope(payload=...), "payload: Field required"),
    ],
)
def test_refuses_what_is_not_an_envelope(raw: bytes | None, reason: str) -> None:
    with pytest.raises(InvalidEventError, match=reason):
        parse_event(raw)


@pytest.mark.parametrize(
    ("event_type", "payload", "reason"),
    [
        ("OrderCreated", {}, "items: Field required"),
        ("OrderCreated", {"items": [{"productId": "p", "quantity": 0}]}, "items.0.quantity"),
        ("OrderCreated", {"items": [{"productId": "p", "quantity": 2**40}]}, "items.0.quantity"),
        ("OrderConfirmed", {"items": [], "totalAmount": "a lot"}, "totalAmount"),
        ("OrderConfirmed", {"items": [], "totalAmount": -1}, "totalAmount"),
        ("StockReserved", {"items": [], "totalAmount": 1e30}, "totalAmount"),
        ("OrderCancelled", {"detail": "no reason given"}, "reason: Field required"),
    ],
)
def test_refuses_a_known_event_with_a_broken_payload(
    event_type: str, payload: dict[str, Any], reason: str
) -> None:
    with pytest.raises(InvalidEventError, match=f"invalid {event_type} payload: .*{reason}"):
        parse_event(envelope(type=event_type, payload=payload))

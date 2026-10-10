import itertools
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from order_analytics.projection import OrderStatus, project
from order_analytics.seed import ANA, KEYBOARD, MOUSE, NOTEBOOK, Ending, ExampleOrder, story
from order_analytics.tables import Event
from tests.support import as_event, custom

ORDER = ExampleOrder("2026-10-05T09:15:00", ANA, ((KEYBOARD, 2), (MOUSE, 1)), Ending.CONFIRMED, 4)
CREATED_AT = datetime(2026, 10, 5, 9, 15, 0, tzinfo=UTC)


def events_of(order: ExampleOrder) -> dict[str, Event]:
    return {message.event_type: as_event(message) for message in story("order-1", order)}


@pytest.mark.parametrize("ending", [Ending.CONFIRMED, Ending.OUT_OF_STOCK, Ending.PAYMENT_REFUSED])
def test_any_subset_of_events_gives_the_same_state_in_every_order(ending: Ending) -> None:
    events = list(events_of(replace(ORDER, ending=ending)).values())

    for size in range(1, len(events) + 1):
        for subset in itertools.combinations(events, size):
            expected = project(subset)
            for arrival in itertools.permutations(subset):
                assert project(arrival) == expected


def test_new_order_is_pending_and_knows_only_what_was_asked() -> None:
    events = events_of(ORDER)

    state = project([events["OrderCreated"]])

    assert state is not None
    assert state.status is OrderStatus.PENDING
    assert (state.customer_id, state.customer_name) == ("c-1001", "Ana Souza")
    assert state.created_at == CREATED_AT
    assert state.finished_at is None
    assert state.total_amount is None
    assert [(item.product_id, item.quantity, item.unit_price) for item in state.items] == [
        (KEYBOARD.id, 2, None),
        (MOUSE.id, 1, None),
    ]


def test_reserved_stock_brings_names_prices_and_total() -> None:
    events = events_of(ORDER)

    state = project([events["OrderCreated"], events["StockReserved"]])

    assert state is not None
    assert state.status is OrderStatus.STOCK_RESERVED
    assert state.total_amount == Decimal("829.70")
    assert [(item.product_name, item.unit_price) for item in state.items] == [
        ("Teclado Mecânico", Decimal("349.90")),
        ("Mouse Sem Fio", Decimal("129.90")),
    ]


def test_confirmed_order_ends_when_the_confirmation_happened() -> None:
    state = project(events_of(ORDER).values())

    assert state is not None
    assert state.status is OrderStatus.CONFIRMED
    assert state.finished_at == CREATED_AT.replace(second=4)
    assert state.total_amount == Decimal("829.70")
    assert state.cancellation_reason is None


def test_order_without_stock_is_cancelled_with_reason_and_no_total() -> None:
    state = project(events_of(replace(ORDER, ending=Ending.OUT_OF_STOCK)).values())

    assert state is not None
    assert state.status is OrderStatus.CANCELLED
    assert state.cancellation_reason == "OUT_OF_STOCK"
    assert state.finished_at == CREATED_AT.replace(second=2)
    assert state.total_amount is None


def test_order_refused_by_payment_keeps_the_total_it_would_have_had() -> None:
    order = replace(ORDER, items=((NOTEBOOK, 1),), ending=Ending.PAYMENT_REFUSED)

    state = project(events_of(order).values())

    assert state is not None
    assert state.status is OrderStatus.CANCELLED
    assert (state.cancellation_reason, state.cancellation_detail) == (
        "PAYMENT_REFUSED",
        "LIMIT_EXCEEDED",
    )
    assert state.total_amount == Decimal("7499.00")


def test_confirmation_that_arrives_alone_already_describes_the_order() -> None:
    state = project([events_of(ORDER)["OrderConfirmed"]])

    assert state is not None
    assert state.status is OrderStatus.CONFIRMED
    assert state.customer_email == "ana@exemplo.com"
    assert state.total_amount == Decimal("829.70")
    assert len(state.items) == 2
    assert state.created_at is None


def test_cancellation_after_the_confirmation_is_ignored() -> None:
    late = custom("OrderCancelled", "order-1", "2026-10-05T09:20:00Z", {"reason": "OUT_OF_STOCK"})

    state = project([*events_of(ORDER).values(), as_event(late)])

    assert state is not None
    assert state.status is OrderStatus.CONFIRMED
    assert state.cancellation_reason is None
    assert state.finished_at == CREATED_AT.replace(second=4)


def test_confirmation_after_the_cancellation_is_ignored() -> None:
    early = custom("OrderCancelled", "order-1", "2026-10-05T09:15:02Z", {"reason": "OUT_OF_STOCK"})

    state = project([*events_of(ORDER).values(), as_event(early)])

    assert state is not None
    assert state.status is OrderStatus.CANCELLED
    assert state.cancellation_reason == "OUT_OF_STOCK"


def test_events_that_do_not_describe_the_order_make_no_row() -> None:
    events = events_of(replace(ORDER, ending=Ending.PAYMENT_REFUSED))

    assert project([events["PaymentRefused"], events["StockReleased"]]) is None
    assert project([]) is None

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)
from pydantic.alias_generators import to_camel


class InvalidEventError(Exception):
    """The message cannot be stored as an event. The text is the reason kept with it."""


def _two_places(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


# Bounded so that a value accepted here always fits the NUMERIC(12, 2) and INTEGER columns.
Money = Annotated[Decimal, Field(ge=0, lt=10**9), AfterValidator(_two_places)]
Quantity = Annotated[int, Field(gt=0, lt=2**31)]


class _Wire(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="ignore")


class Envelope(_Wire):
    event_id: str = Field(min_length=1)
    type: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    occurred_at: AwareDatetime
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def in_utc(cls, value: datetime) -> datetime:
        try:
            return value.astimezone(UTC)
        except OverflowError:
            raise ValueError("date out of range") from None


class Item(_Wire):
    product_id: str = Field(min_length=1)
    quantity: Quantity
    product_name: str | None = None
    unit_price: Money | None = None


class WithCustomer(_Wire):
    customer_id: str | None = None
    customer_name: str | None = None
    customer_email: str | None = None


class OrderCreated(WithCustomer):
    items: list[Item]


class OrderConfirmed(WithCustomer):
    total_amount: Money
    items: list[Item]


class OrderCancelled(WithCustomer):
    reason: str = Field(min_length=1)
    detail: str | None = None


class StockReserved(_Wire):
    total_amount: Money
    items: list[Item]


# Only these change the read model. Any other type is stored and left alone.
PAYLOADS: dict[str, type[_Wire]] = {
    "OrderCreated": OrderCreated,
    "OrderConfirmed": OrderConfirmed,
    "OrderCancelled": OrderCancelled,
    "StockReserved": StockReserved,
}


def parse_event(raw: bytes | None) -> Envelope:
    if not raw:
        raise InvalidEventError("empty message")
    try:
        envelope = Envelope.model_validate_json(raw)
    except ValidationError as error:
        raise InvalidEventError(f"invalid envelope: {_describe(error)}") from None

    payload = PAYLOADS.get(envelope.type)
    if payload is not None:
        try:
            payload.model_validate(envelope.payload)
        except ValidationError as error:
            raise InvalidEventError(
                f"invalid {envelope.type} payload: {_describe(error)}"
            ) from None
    return envelope


def _describe(error: ValidationError) -> str:
    problems = error.errors(include_url=False, include_input=False)
    if problems[0]["type"] == "json_invalid":
        return "not valid JSON"
    return "; ".join(
        f"{'.'.join(str(part) for part in problem['loc'])}: {problem['msg']}"
        for problem in problems
    )

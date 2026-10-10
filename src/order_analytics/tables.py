from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Numeric,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Event(Base):
    """Every valid event received, exactly once. The read model is derived from this table."""

    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    type: Mapped[str] = mapped_column(Text)
    order_id: Mapped[str] = mapped_column(Text, index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    kafka_topic: Mapped[str] = mapped_column(Text)
    kafka_partition: Mapped[int]
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class RejectedEvent(Base):
    __tablename__ = "rejected_events"
    # A message redelivered after a crash must not be recorded twice.
    __table_args__ = (UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    kafka_topic: Mapped[str] = mapped_column(Text)
    kafka_partition: Mapped[int]
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    message_key: Mapped[str | None] = mapped_column(Text)
    message_value: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text)
    customer_id: Mapped[str | None] = mapped_column(Text)
    customer_name: Mapped[str | None] = mapped_column(Text)
    customer_email: Mapped[str | None] = mapped_column(Text)
    total_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    cancellation_reason: Mapped[str | None] = mapped_column(Text)
    cancellation_detail: Mapped[str | None] = mapped_column(Text)
    # Null until OrderCreated arrives, which may be after the other events.
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class OrderItem(Base):
    __tablename__ = "order_items"

    order_id: Mapped[str] = mapped_column(
        ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[str] = mapped_column(Text)
    product_name: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[int]
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))

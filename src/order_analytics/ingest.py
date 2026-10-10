import logging
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.events import PAYLOADS, Envelope, InvalidEventError, parse_event
from order_analytics.read_model import refresh_order
from order_analytics.tables import Event, RejectedEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Message:
    """What is needed from a Kafka record, so that storing does not depend on Kafka."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class Outcome(StrEnum):
    STORED = "stored"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


async def ingest(sessions: async_sessionmaker[AsyncSession], message: Message) -> Outcome:
    """Store one message and bring the read model up to date, in a single transaction.

    Returning normally means the message is dealt with for good and its offset may be
    committed. Any exception means nothing was written and the message must be retried.
    """
    try:
        envelope = parse_event(message.value)
    except InvalidEventError as invalid:
        await _reject(sessions, message, str(invalid))
        return Outcome.REJECTED

    context = {"orderId": envelope.order_id, "eventId": envelope.event_id, "type": envelope.type}
    try:
        async with sessions.begin() as session:
            if not await _store(session, message, envelope):
                logger.info("duplicate event ignored", extra=context)
                return Outcome.DUPLICATE
            if envelope.type in PAYLOADS:
                # Events of one order come from three topics, so two instances of this
                # service may hold events of the same order at the same moment. The lock
                # makes the second one wait and then see what the first one stored.
                await session.execute(
                    select(func.pg_advisory_xact_lock(func.hashtextextended(envelope.order_id, 0)))
                )
                await refresh_order(session, envelope.order_id)
    except DBAPIError as error:
        if not _is_data_refused(error):
            raise
        await _reject(sessions, message, f"refused by the database: {error.orig}")
        return Outcome.REJECTED

    logger.info("event stored", extra=context)
    return Outcome.STORED


async def _store(session: AsyncSession, message: Message, envelope: Envelope) -> bool:
    inserted = await session.scalar(
        insert(Event)
        .values(
            event_id=envelope.event_id,
            type=envelope.type,
            order_id=envelope.order_id,
            occurred_at=envelope.occurred_at,
            payload=envelope.payload,
            kafka_topic=message.topic,
            kafka_partition=message.partition,
            kafka_offset=message.offset,
        )
        .on_conflict_do_nothing(index_elements=[Event.event_id])
        .returning(Event.event_id)
    )
    return inserted is not None


async def _reject(
    sessions: async_sessionmaker[AsyncSession], message: Message, reason: str
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            insert(RejectedEvent)
            .values(
                kafka_topic=message.topic,
                kafka_partition=message.partition,
                kafka_offset=message.offset,
                message_key=_as_text(message.key),
                message_value=_as_text(message.value),
                reason=reason,
            )
            .on_conflict_do_nothing()
        )
    logger.warning(
        "message rejected",
        extra={
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "reason": reason,
        },
    )


def _as_text(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    # PostgreSQL text cannot hold NUL, and a rejected message can be any bytes at all.
    return raw.decode("utf-8", errors="replace").replace("\x00", "�")


def _is_data_refused(error: DBAPIError) -> bool:
    # SQLSTATE class 22 is "data exception": the value itself is the problem (a NUL inside
    # a string, for instance), so trying again would fail forever and block the partition.
    sqlstate = getattr(error.orig, "sqlstate", None)
    return isinstance(sqlstate, str) and sqlstate.startswith("22")

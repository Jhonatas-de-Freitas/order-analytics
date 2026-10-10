import asyncio
import contextlib
import logging

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import (
    CommitFailedError,
    IllegalStateError,
    KafkaError,
    TopicAlreadyExistsError,
    for_code,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.ingest import Message, ingest

logger = logging.getLogger(__name__)

TOPICS = ("orders.events", "inventory.events", "payments.events")
GROUP_ID = "order-analytics"
PARTITIONS_PER_TOPIC = 3

_POLL_TIMEOUT_MS = 1000
_BATCH_SIZE = 100
_RETRY_SECONDS = 3


class EventConsumer:
    """Reads the three topics and stores what arrives, committing offsets only after that."""

    def __init__(self, bootstrap_servers: str, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._sessions = sessions
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._connected = False

    @property
    def running(self) -> bool:
        return self._connected

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="event-consumer")

    async def stop(self) -> None:
        """Finish the message in hand, commit what was stored and leave the group."""
        self._stopping.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._consume()
            except KafkaError as error:
                logger.warning("kafka unavailable, will reconnect", extra={"error": str(error)})
            except Exception:
                logger.exception("consumer failed, will reconnect")
            await self._pause()

    async def _consume(self) -> None:
        await _ensure_topics(self._bootstrap_servers)
        consumer = AIOKafkaConsumer(
            *TOPICS,
            bootstrap_servers=self._bootstrap_servers,
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            self._connected = True
            logger.info("consumer started", extra={"topics": TOPICS})
            while not self._stopping.is_set():
                batches = await consumer.getmany(
                    timeout_ms=_POLL_TIMEOUT_MS, max_records=_BATCH_SIZE
                )
                all_stored = await self._store(consumer, batches)
                if not all_stored:
                    await self._pause()
        finally:
            self._connected = False
            await consumer.stop()
            logger.info("consumer stopped")

    async def _store(
        self, consumer: AIOKafkaConsumer, batches: dict[TopicPartition, list[ConsumerRecord]]
    ) -> bool:
        next_offsets: dict[TopicPartition, int] = {}
        all_stored = True
        for partition, records in batches.items():
            for record in records:
                if self._stopping.is_set():
                    break
                message = Message(
                    record.topic, record.partition, record.offset, record.key, record.value
                )
                try:
                    await ingest(self._sessions, message)
                except Exception:
                    logger.exception(
                        "could not store message, will retry",
                        extra={
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                        },
                    )
                    # Go back to this message: skipping it would lose the event.
                    consumer.seek(partition, message.offset)
                    all_stored = False
                    break
                next_offsets[partition] = message.offset + 1

        if next_offsets:
            try:
                await consumer.commit(next_offsets)
            except (CommitFailedError, IllegalStateError) as error:
                # A rebalance took the partitions away. Whoever owns them now reads these
                # events again and finds them already stored.
                logger.warning("offsets not committed", extra={"error": str(error)})
        return all_stored

    async def _pause(self) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), _RETRY_SECONDS)


async def _ensure_topics(bootstrap_servers: str) -> None:
    # This service may come up before the ones that publish. Creating the topics here,
    # with the partition count of the contract, means it does not matter who starts first.
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        await admin.start()
        missing = sorted(set(TOPICS) - set(await admin.list_topics()))
        if not missing:
            return
        response = await admin.create_topics(
            [NewTopic(name, PARTITIONS_PER_TOPIC, replication_factor=1) for name in missing]
        )
        # The broker answers topic by topic, and aiokafka hands the answer over as it came:
        # a refusal is an error code in it, not an exception.
        created = []
        for topic, code, *detail in response.topic_errors:
            if code == 0:
                created.append(topic)
            elif code != TopicAlreadyExistsError.errno:
                error = for_code(code)
                reason = detail[0] if detail and detail[0] else error.description
                raise error(f"topic {topic} not created: {reason}")
        if created:
            logger.info("topics created", extra={"topics": created})
    finally:
        await admin.close()

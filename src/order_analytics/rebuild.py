"""Throws the read model away and derives it again from the stored events.

Run with `python -m order_analytics.rebuild`.
"""

import asyncio
import logging

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from order_analytics.config import Settings
from order_analytics.events import PAYLOADS
from order_analytics.logs import configure_logging
from order_analytics.read_model import refresh_order
from order_analytics.tables import Event, Order

# Not __name__: this module is run with `python -m`, where that would be "__main__".
logger = logging.getLogger("order_analytics.rebuild")


async def rebuild(session: AsyncSession) -> int:
    """Rebuild every order inside the caller's transaction. Returns how many there are.

    Safe to run while the service is consuming. The table lock makes the consumer wait
    at its first write to `orders` until this commits, and what it writes then was
    computed from every event of that order, so nothing stored meanwhile is lost.
    Readers are not blocked: they see the old rows until the commit.
    """
    await session.execute(text("LOCK TABLE orders IN SHARE ROW EXCLUSIVE MODE"))
    await session.execute(delete(Order))
    order_ids = await session.scalars(
        select(Event.order_id).where(Event.type.in_(PAYLOADS)).distinct()
    )
    rebuilt = 0
    for order_id in order_ids.all():
        await refresh_order(session, order_id)
        rebuilt += 1
    return rebuilt


async def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    engine = create_async_engine(settings.database_url)
    async with AsyncSession(engine) as session, session.begin():
        orders = await rebuild(session)
    await engine.dispose()
    logger.info("read model rebuilt", extra={"orders": orders})


if __name__ == "__main__":
    asyncio.run(main())

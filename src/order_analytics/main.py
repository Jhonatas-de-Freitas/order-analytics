from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from order_analytics.api import router
from order_analytics.config import Settings
from order_analytics.consumer import EventConsumer

DESCRIPTION = """
Reads the order events from Kafka (`orders.events`, `inventory.events`, `payments.events`),
keeps one row per order and answers with numbers: how many orders, how much revenue,
what was cancelled and why, which products sell the most.
"""


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        consumer = (
            EventConsumer(settings.kafka_bootstrap_servers, sessions)
            if settings.kafka_enabled
            else None
        )
        app.state.sessions = sessions
        app.state.consumer = consumer

        if consumer:
            consumer.start()
        yield
        # The consumer goes first, while it still has a database to finish its work on.
        if consumer:
            await consumer.stop()
        await engine.dispose()

    app = FastAPI(
        title="order-analytics", version="0.1.0", description=DESCRIPTION, lifespan=lifespan
    )
    app.include_router(router)
    return app


app = create_app()

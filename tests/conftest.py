import os
from collections.abc import AsyncIterator, Callable, Iterator

import pytest
from alembic import command
from alembic.config import Config
from docker.errors import DockerException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from order_analytics.config import Settings
from order_analytics.main import create_app


def _start[C: DockerContainer](start: Callable[[], C]) -> C:
    try:
        return start()
    except DockerException:
        # On a machine without Docker these tests are skipped. On CI that would hide them.
        if os.environ.get("CI"):
            raise
        pytest.skip("Docker is not available")


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """A migrated PostgreSQL. TEST_DATABASE_URL points at an existing one instead of a container."""
    url = os.environ.get("TEST_DATABASE_URL")
    postgres = None
    if not url:
        postgres = _start(lambda: PostgresContainer("postgres:17-alpine", driver="asyncpg").start())
        url = postgres.get_connection_url()

    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(config, "head")
    yield url
    if postgres:
        postgres.stop()


@pytest.fixture
async def sessions(database_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE events, rejected_events, orders CASCADE"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def client(
    database_url: str, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[AsyncClient]:
    """The API without the Kafka consumer, on the database that `sessions` has just emptied."""
    app = create_app(Settings(database_url=database_url, kafka_enabled=False))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app), base_url="http://test") as http,
    ):
        yield http


@pytest.fixture
def kafka() -> Iterator[str]:
    """Bootstrap address of a broker started for this test alone, with nothing left by others."""
    container = _start(lambda: KafkaContainer().with_kraft().start(timeout=90))
    yield container.get_bootstrap_server()
    container.stop()

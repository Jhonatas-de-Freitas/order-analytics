from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from order_analytics.tables import Base


async def test_migrations_create_exactly_what_the_models_describe(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as session:
        connection = await session.connection()
        differences = await connection.run_sync(
            lambda sync: compare_metadata(MigrationContext.configure(sync), Base.metadata)
        )

    assert differences == []

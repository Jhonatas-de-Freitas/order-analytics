from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/analytics_db"
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_enabled: bool = True
    port: int = 8090
    log_level: str = "INFO"

    @field_validator("database_url")
    @classmethod
    def use_async_driver(cls, value: str) -> str:
        # Compose files usually carry the plain scheme; the engine here is always asyncpg.
        for scheme in ("postgresql://", "postgres://"):
            if value.startswith(scheme):
                return "postgresql+asyncpg://" + value.removeprefix(scheme)
        return value

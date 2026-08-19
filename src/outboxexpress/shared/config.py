"""Typed, environment-driven settings shared by every service."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OUTBOX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # No Kafka settings here. The api is the only M1 service and it has no broker.
    database_url: str = "postgresql+psycopg://outbox:outbox@localhost:5432/outbox"
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    log_level: str = "INFO"
    log_json: bool = True


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()

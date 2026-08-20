"""Typed, environment-driven settings shared by every service."""

from functools import lru_cache
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OUTBOX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+psycopg://outbox:outbox@localhost:5432/outbox"
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    log_level: str = "INFO"
    log_json: bool = True

    # The api has no Kafka client and no Kafka configuration (spec 4). These serve
    # the relay, and from M3 the notifier.
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "orders.events"
    kafka_delivery_timeout_ms: int = 30_000

    relay_batch_size: int = 100
    relay_lease_seconds: int = 60
    relay_poll_interval_seconds: float = 0.5

    # Spec 7.1. Full jitter draws uniformly below a ceiling that doubles per attempt
    # and stops at the cap; tests shrink both so a backoff does not cost real seconds.
    relay_backoff_base_seconds: float = 1.0
    relay_backoff_cap_seconds: float = 60.0

    @model_validator(mode="after")
    def _delivery_must_time_out_inside_the_lease(self) -> Self:
        """Spec 7.1's hard constraint. Tests shrink both, so it is checked, not pinned."""
        if self.kafka_delivery_timeout_ms >= self.relay_lease_seconds * 1000:
            raise ValueError(
                f"kafka_delivery_timeout_ms ({self.kafka_delivery_timeout_ms}) must be "
                f"below relay_lease_seconds ({self.relay_lease_seconds}s): a publish "
                "outliving its lease is reclaimed by another relay and published twice"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()

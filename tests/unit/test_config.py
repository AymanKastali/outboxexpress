import pytest
from pydantic import ValidationError

from outboxexpress.shared.config import Settings, get_settings


def test_settings_read_the_outbox_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOX_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    monkeypatch.setenv("OUTBOX_LOG_LEVEL", "DEBUG")
    settings = Settings()
    assert settings.database_url == "postgresql+psycopg://u:p@db:5432/x"
    assert settings.log_level == "DEBUG"


def test_get_settings_is_cached() -> None:
    assert get_settings() is get_settings()


def test_relay_settings_carry_the_values_the_spec_fixes() -> None:
    settings = Settings()
    assert settings.kafka_topic == "orders.events"
    assert settings.relay_batch_size == 100
    assert settings.relay_lease_seconds == 60
    assert settings.kafka_delivery_timeout_ms == 30_000
    assert settings.relay_backoff_base_seconds == 1.0
    assert settings.relay_backoff_cap_seconds == 60.0


def test_a_delivery_timeout_at_or_above_the_lease_is_refused() -> None:
    # Spec 7.1: a publish still in flight when the lease expires is reclaimed by
    # another relay and published twice. The config must not be able to say that.
    with pytest.raises(ValidationError, match="delivery_timeout"):
        Settings(kafka_delivery_timeout_ms=60_000, relay_lease_seconds=60)

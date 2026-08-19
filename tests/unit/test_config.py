import pytest

from outboxexpress.shared.config import Settings, get_settings


def test_settings_read_the_outbox_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOX_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    monkeypatch.setenv("OUTBOX_LOG_LEVEL", "DEBUG")
    settings = Settings()
    assert settings.database_url == "postgresql+psycopg://u:p@db:5432/x"
    assert settings.log_level == "DEBUG"


def test_get_settings_is_cached() -> None:
    assert get_settings() is get_settings()

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_migration_creates_the_four_tables(sync_engine: Engine) -> None:
    tables = set(inspect(sync_engine).get_table_names())
    assert {"orders", "outbox", "idempotency_keys", "processed_events"} <= tables


def test_outbox_partial_indexes_exist_in_postgres(sync_engine: Engine) -> None:
    with sync_engine.connect() as connection:
        rows = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'outbox'")
            ).all()
        }
    assert "WHERE (status = 'pending'" in rows["ix_outbox_pending"]
    assert "WHERE (status = 'publishing'" in rows["ix_outbox_reclaim"]


def test_downgrade_then_upgrade_is_clean(sync_engine: Engine) -> None:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    assert "outbox" in inspect(sync_engine).get_table_names()

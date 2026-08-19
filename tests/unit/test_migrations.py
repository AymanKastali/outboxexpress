from sqlalchemy import Engine, inspect, text


def test_migration_creates_the_four_tables(sync_engine: Engine):
    tables = set(inspect(sync_engine).get_table_names())
    assert {"orders", "outbox", "idempotency_keys", "processed_events"} <= tables


def test_outbox_partial_indexes_exist_in_postgres(sync_engine: Engine):
    with sync_engine.connect() as connection:
        rows = dict(
            connection.execute(
                text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'outbox'")
            ).all()
        )
    assert "WHERE (status = 'pending'" in rows["ix_outbox_pending"]
    assert "WHERE (status = 'publishing'" in rows["ix_outbox_reclaim"]


def test_downgrade_then_upgrade_is_clean(sync_engine: Engine):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    assert "outbox" in inspect(sync_engine).get_table_names()

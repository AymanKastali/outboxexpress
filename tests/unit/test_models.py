from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from outboxexpress.shared.models import Base, OutboxStatus, new_id


def test_the_four_tables_are_registered() -> None:
    assert set(Base.metadata.tables) == {
        "orders",
        "outbox",
        "idempotency_keys",
        "processed_events",
    }


def test_outbox_indexes_are_partial() -> None:
    indexes = {str(ix.name): ix for ix in Base.metadata.tables["outbox"].indexes}
    pending = str(CreateIndex(indexes["ix_outbox_pending"]).compile(dialect=postgresql.dialect()))
    reclaim = str(CreateIndex(indexes["ix_outbox_reclaim"]).compile(dialect=postgresql.dialect()))
    assert "next_attempt_at" in pending and "status = 'pending'" in pending
    assert "locked_until" in reclaim and "status = 'publishing'" in reclaim


def test_outbox_status_serialises_as_lowercase_values() -> None:
    assert OutboxStatus.PENDING == "pending"
    assert [s.value for s in OutboxStatus] == ["pending", "publishing", "published", "dead"]


def test_new_id_returns_time_ordered_uuid7() -> None:
    first, second = new_id(), new_id()
    assert first.version == 7
    assert first < second  # v7 sorts by creation time

"""The relay's two state transitions over the outbox table."""

from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outboxexpress.relay.outbox import claim_batch, mark_published
from outboxexpress.shared.models import utcnow
from pending_events import write_pending_events

LEASE = 60


def row_state(engine: Engine, event_id: UUID) -> dict[str, Any]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    "SELECT status, attempts, locked_by, locked_until, published_at "
                    "FROM outbox WHERE id = :id"
                ),
                {"id": event_id},
            )
            .mappings()
            .one()
        )


def test_a_claim_takes_the_lease_and_counts_the_attempt(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claimed = claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == [event_id]
    state = row_state(sync_engine, event_id)
    assert state["status"] == "publishing"
    assert state["attempts"] == 1
    assert state["locked_by"] == "relay-1"
    assert state["locked_until"] > utcnow()


def test_a_claim_returns_the_event_the_publisher_needs(
    relay_sessions: sessionmaker[Session],
) -> None:
    write_pending_events(relay_sessions, count=1, aggregate_id="order-7")

    with relay_sessions() as session:
        (event,) = claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    assert event.aggregate_id == "order-7"
    assert event.event_type == "OrderCreated"
    assert event.payload == {"sequence": 0}


def test_a_claim_stops_at_the_batch_size_and_takes_the_oldest_first(
    relay_sessions: sessionmaker[Session],
) -> None:
    written = write_pending_events(relay_sessions, count=5)

    with relay_sessions() as session:
        claimed = claim_batch(session, instance_id="relay-1", batch_size=3, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == written[:3]


def test_a_row_not_yet_due_is_left_alone(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with sync_engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox SET next_attempt_at = now() + interval '1 hour' WHERE id = :id"),
            {"id": event_id},
        )

    with relay_sessions() as session:
        assert claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE) == []


def test_two_relays_claiming_at_once_get_disjoint_rows(
    relay_sessions: sessionmaker[Session],
) -> None:
    # The SKIP LOCKED contract. I3 proves it across two processes in plan 2; here it is
    # the unit behaviour of the statement itself.
    write_pending_events(relay_sessions, count=6)

    with relay_sessions() as first, relay_sessions() as second:
        one = claim_batch(first, instance_id="relay-1", batch_size=3, lease_seconds=LEASE)
        two = claim_batch(second, instance_id="relay-2", batch_size=3, lease_seconds=LEASE)

    assert len(one) == 3 and len(two) == 3
    assert {event.event_id for event in one}.isdisjoint({event.event_id for event in two})


def test_marking_published_clears_the_lease(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
        assert mark_published(session, [event_id], instance_id="relay-1") == 1

    state = row_state(sync_engine, event_id)
    assert state["status"] == "published"
    assert state["published_at"] is not None
    assert state["locked_by"] is None
    assert state["locked_until"] is None


def test_a_relay_cannot_mark_a_row_another_relay_holds(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # The slow relay whose lease expired must not overwrite the outcome of the relay
    # that took the row from it. Guarding on locked_by is what makes that impossible.
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
        assert mark_published(session, [event_id], instance_id="relay-2") == 0

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_marking_nothing_touches_nothing(relay_sessions: sessionmaker[Session]) -> None:
    with relay_sessions() as session:
        assert mark_published(session, [], instance_id="relay-1") == 0

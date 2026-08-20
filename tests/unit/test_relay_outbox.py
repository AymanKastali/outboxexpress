"""The relay's state transitions over the outbox table."""

from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outbox_state import expire_leases, statuses
from outboxexpress.relay.outbox import (
    FailedEvent,
    claim_batch,
    mark_published,
    reclaim_expired,
    reschedule_failed,
    retry_ceiling_seconds,
    retry_delay,
)
from outboxexpress.shared.models import utcnow
from pending_events import write_pending_events

LEASE = 60
BASE = 1.0
CAP = 60.0


def row_state(engine: Engine, event_id: UUID) -> dict[str, Any]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    "SELECT status, attempts, next_attempt_at, locked_by, locked_until, "
                    "last_error, published_at FROM outbox WHERE id = :id"
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


def test_a_claimed_row_is_not_offered_to_the_next_claim(
    relay_sessions: sessionmaker[Session],
) -> None:
    # Sequential on purpose: claim_batch commits before returning, so this pins the
    # status guard, not SKIP LOCKED. Two relays genuinely overlapping is I3's job.
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


def test_an_expired_lease_returns_the_row_to_pending(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=10) == 1

    state = row_state(sync_engine, event_id)
    assert state["status"] == "pending"
    assert state["locked_by"] is None
    assert state["locked_until"] is None
    # The attempt was already counted when the row was claimed. Counting it again here
    # would make `attempts` say two deliveries were tried when only one was.
    assert state["attempts"] == 1


def test_a_live_lease_is_left_alone(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # The lease's whole purpose: a relay still inside its window keeps its row, so a
    # slow publish is never republished underneath the process performing it.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=10) == 0

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_a_reclaim_stops_at_the_batch_size(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    write_pending_events(relay_sessions, count=5)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=5, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=3) == 3

    assert statuses(sync_engine).count("pending") == 3


def test_a_reclaimed_row_is_claimable_by_another_relay(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # Recovery is only worth anything if the row can then be published. This is the
    # join between the reclaim and the claim, and it is what I1 rests on.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        reclaim_expired(session, instance_id="relay-2", batch_size=10)
        claimed = claim_batch(session, instance_id="relay-2", batch_size=10, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == [event_id]
    assert row_state(sync_engine, event_id)["locked_by"] == "relay-2"


def test_the_backoff_ceiling_doubles_with_each_attempt() -> None:
    # attempts is already 1 when the first publish fails, because the claim counted it.
    # So attempt 1 waits within 1s, per spec 7.1's "1 s, x2".
    ceilings = [
        retry_ceiling_seconds(attempts, base_seconds=BASE, cap_seconds=CAP)
        for attempts in range(1, 6)
    ]
    assert ceilings == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_the_backoff_ceiling_stops_at_the_cap() -> None:
    assert retry_ceiling_seconds(20, base_seconds=BASE, cap_seconds=CAP) == CAP


def test_a_long_outage_does_not_overflow_the_ceiling() -> None:
    # attempts has no ceiling by design: a retriable failure retries forever. At one
    # attempt per delivery timeout, 2000 of them is under a day -- and 1.0 * 2**2000
    # raises OverflowError, so the exponent clamp is load-bearing, not defensive.
    assert retry_ceiling_seconds(2_000, base_seconds=BASE, cap_seconds=CAP) == CAP


def test_the_delay_is_jittered_inside_the_ceiling() -> None:
    delays = {retry_delay(4, base_seconds=BASE, cap_seconds=CAP) for _ in range(50)}

    # Jitter is the whole point: without it, N relays that failed on one broker
    # outage would come back in lockstep and fail together again.
    assert len(delays) > 1
    assert all(timedelta() <= delay <= timedelta(seconds=8) for delay in delays)


def test_a_retriable_failure_goes_back_to_pending_with_its_error_recorded(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        moved = reschedule_failed(
            session,
            [FailedEvent(event_id=event_id, error="Local: Message timed out")],
            instance_id="relay-1",
            base_seconds=BASE,
            cap_seconds=CAP,
        )

    assert moved == 1
    state = row_state(sync_engine, event_id)
    assert state["status"] == "pending"
    assert state["last_error"] == "Local: Message timed out"
    assert state["locked_by"] is None
    assert state["locked_until"] is None
    # Counted once, by the claim. The next claim counts the next attempt.
    assert state["attempts"] == 1


def test_a_rescheduled_row_is_due_no_later_than_its_ceiling(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    before = utcnow()

    with relay_sessions() as session:
        reschedule_failed(
            session,
            [FailedEvent(event_id=event_id, error="boom")],
            instance_id="relay-1",
            base_seconds=BASE,
            cap_seconds=CAP,
        )

    # One bound, with slack. Full jitter promises no minimum wait, so there is no
    # lower bound to assert; and `before` comes off the host clock while
    # next_attempt_at comes off the container's, so the upper bound carries a second
    # of tolerance rather than pretending the two clocks are one.
    due = row_state(sync_engine, event_id)["next_attempt_at"]
    assert due <= before + timedelta(seconds=BASE + 1)


def test_a_relay_cannot_reschedule_a_row_another_relay_holds(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # Same guard, and the same reason, as mark_published: a relay whose lease expired
    # mid-publish must not push back a row another relay has since taken and may
    # already have published.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        assert (
            reschedule_failed(
                session,
                [FailedEvent(event_id=event_id, error="boom")],
                instance_id="relay-2",
                base_seconds=BASE,
                cap_seconds=CAP,
            )
            == 0
        )

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_rescheduling_nothing_touches_nothing(relay_sessions: sessionmaker[Session]) -> None:
    with relay_sessions() as session:
        assert (
            reschedule_failed(
                session, [], instance_id="relay-1", base_seconds=BASE, cap_seconds=CAP
            )
            == 0
        )

"""I3: two relays never hold one outbox row at the same time.

Both handovers are here -- the claim that takes a fresh row, and the reclaim that takes
an abandoned one. The two relays run in threads released together, because a test that
lets one finish before the other starts never exercises the concurrent path at all: it
would pass on the status change alone.

What these tests guard is the single-statement shape of both transitions. Postgres
supplies the actual mutual exclusion, so the failure they exist to catch is a future
refactor that splits either statement into a read and then a write.
"""

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outbox_state import expire_leases, statuses
from outboxexpress.relay.outbox import ClaimedEvent, claim_batch, reclaim_expired
from pending_events import write_pending_events

BATCH = 100
# Twice the batch, so two relays taking a full batch each covers the table exactly.
TOTAL = BATCH * 2
LEASE = 60


def in_parallel[T](work: Callable[[str], T], *instance_ids: str) -> list[T]:
    """Run ``work`` once per instance id, all released at the same moment."""
    start = threading.Barrier(len(instance_ids))

    def run(instance_id: str) -> T:
        start.wait()
        return work(instance_id)

    # ThreadPoolExecutor rather than bare Threads: map() re-raises whatever a worker
    # raised, so a broken statement fails the test instead of vanishing into a thread.
    with ThreadPoolExecutor(max_workers=len(instance_ids)) as pool:
        return list(pool.map(run, instance_ids))


def holders(engine: Engine) -> dict[str, int]:
    """How many rows each relay instance currently holds a lease on."""
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT locked_by, count(*) AS held FROM outbox "
                "WHERE status = 'publishing' GROUP BY locked_by"
            )
        ).all()
    return {row.locked_by: row.held for row in rows}


def test_two_relays_claiming_at_once_never_share_a_row(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    written = write_pending_events(relay_sessions, count=TOTAL)

    def claim(instance_id: str) -> list[ClaimedEvent]:
        with relay_sessions() as session:
            return claim_batch(
                session, instance_id=instance_id, batch_size=BATCH, lease_seconds=LEASE
            )

    first, second = in_parallel(claim, "relay-1", "relay-2")

    one = {event.event_id for event in first}
    two = {event.event_id for event in second}
    assert one.isdisjoint(two)
    assert one | two == set(written)
    # And the table agrees with what the two relays believe they took.
    assert holders(sync_engine) == {"relay-1": BATCH, "relay-2": BATCH}


def test_two_relays_reclaiming_at_once_never_recover_one_row_twice(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    write_pending_events(relay_sessions, count=TOTAL)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=TOTAL, lease_seconds=LEASE)
    expire_leases(sync_engine)

    def reclaim(instance_id: str) -> int:
        with relay_sessions() as session:
            return reclaim_expired(session, instance_id=instance_id, batch_size=BATCH)

    counts = in_parallel(reclaim, "relay-2", "relay-3")

    # The two assertions together are the guarantee. Equal counts alone would also hold
    # if both relays recovered the same hundred rows -- the second assertion is what
    # rules that out, because a hundred rows would still be sitting in publishing.
    assert sum(counts) == TOTAL
    assert statuses(sync_engine) == ["pending"] * TOTAL
    assert holders(sync_engine) == {}

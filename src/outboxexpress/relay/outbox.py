"""Every state transition the relay makes to the outbox table.

The claim and the mark are two halves of one sequence over one table, so they share a
module and a reason to change; the reclaim and the dead-letter join them in later
plans. Each function owns its transaction and holds it for a single statement -- no
lock in this file is ever held across the network call in ``publisher``.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from outboxexpress.shared.logging import get_logger
from outboxexpress.shared.models import Outbox, OutboxStatus

log = get_logger(__name__)


@dataclass(frozen=True)
class ClaimedEvent:
    """One outbox row, reduced to what the publisher needs and nothing else."""

    event_id: UUID
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]


def claim_batch(
    session: Session, *, instance_id: str, batch_size: int, lease_seconds: int
) -> list[ClaimedEvent]:
    """Take a lease on up to ``batch_size`` due rows and return them.

    One statement, so the select and the update cannot be separated by another relay.
    ``SKIP LOCKED`` is what lets N relays share the table: a row already locked by a
    peer is passed over rather than waited for.
    """
    due = (
        select(Outbox.id)
        .where(Outbox.status == OutboxStatus.PENDING, Outbox.next_attempt_at <= func.now())
        # next_attempt_at leads because ix_outbox_pending is ordered by it; id breaks
        # ties, and being UUIDv7 it breaks them in write order, which is what keeps a
        # single aggregate's events in sequence (I5).
        .order_by(Outbox.next_attempt_at, Outbox.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    claim = (
        update(Outbox)
        .where(Outbox.id.in_(due))
        .values(
            status=OutboxStatus.PUBLISHING,
            locked_by=instance_id,
            locked_until=func.now() + timedelta(seconds=lease_seconds),
            attempts=Outbox.attempts + 1,
        )
        .returning(Outbox.id, Outbox.aggregate_id, Outbox.event_type, Outbox.payload)
        # The default here is 'auto', which on PostgreSQL means 'fetch': expire the
        # matched rows on any objects the Session is holding. This relay never loads
        # Outbox objects -- it reads the Rows below -- so opting out says that plainly
        # rather than asking the ORM to reconcile an empty identity map.
        .execution_options(synchronize_session=False)
    )
    with session.begin():
        rows = session.execute(claim).all()

    claimed = [
        ClaimedEvent(
            event_id=row.id,
            aggregate_id=row.aggregate_id,
            event_type=row.event_type,
            payload=row.payload,
        )
        for row in rows
    ]
    if claimed:
        log.info("outbox_claimed", locked_by=instance_id, count=len(claimed))
    return claimed


def mark_published(session: Session, event_ids: Sequence[UUID], *, instance_id: str) -> int:
    """Retire the rows this relay published. Returns how many it actually retired.

    The ``locked_by`` guard is load-bearing: a relay whose lease expired mid-publish
    must not overwrite the row that another relay has since taken from it.
    """
    if not event_ids:
        return 0
    mark = (
        update(Outbox)
        .where(
            Outbox.id.in_(event_ids),
            Outbox.status == OutboxStatus.PUBLISHING,
            Outbox.locked_by == instance_id,
        )
        .values(
            status=OutboxStatus.PUBLISHED,
            published_at=func.now(),
            locked_by=None,
            locked_until=None,
            # Nothing writes last_error until plan 3 adds error classification, but the
            # column is already here: clearing it means `published` can never carry the
            # error from an earlier attempt that was later retried successfully.
            last_error=None,
        )
        # RETURNING rather than rowcount: pyright strict rejects Result.rowcount, which
        # is a CursorResult member that Session.execute's return type does not expose.
        .returning(Outbox.id)
        .execution_options(synchronize_session=False)
    )
    with session.begin():
        marked = len(session.execute(mark).scalars().all())
    log.info("outbox_published", locked_by=instance_id, count=marked)
    return marked

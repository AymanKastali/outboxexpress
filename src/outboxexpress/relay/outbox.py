"""Every state transition the relay makes to the outbox table.

The claim and the mark are two halves of one sequence over one table, so they share a
module and a reason to change; the reclaim and the dead-letter join them in later
plans. Each function owns its transaction and holds it for a single statement -- no
lock in this file is ever held across the network call in ``publisher``.
"""

import random
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


@dataclass(frozen=True)
class FailedEvent:
    """One rejected event, and what the broker said about it.

    ``error`` is already rendered to text: the classification happened in
    ``publisher``, and this module has no business re-reading a ``KafkaError``.
    """

    event_id: UUID
    error: str


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


def reclaim_expired(session: Session, *, instance_id: str, batch_size: int) -> int:
    """Return abandoned rows to ``pending``. Returns how many were recovered.

    A row sits in ``publishing`` with an expired lease only because the relay holding
    it died, or its publish outlived the lease. Either way the event was never retired,
    so it goes back on the queue rather than waiting for a human to notice.

    ``SKIP LOCKED`` for the same reason the claim uses it: so a relay never blocks
    behind a peer that is mid-reclaim. It is *not* what stops two relays recovering one
    row -- the row lock and the ``status='publishing'`` predicate do that, because a
    peer that waited on the lock re-reads the row as ``pending`` and passes over it.
    Measured on Postgres 18: drop ``SKIP LOCKED`` and the second relay still recovers a
    correct disjoint batch, it just waits out the first relay's transaction to do it.
    """
    abandoned = (
        select(Outbox.id)
        .where(Outbox.status == OutboxStatus.PUBLISHING, Outbox.locked_until < func.now())
        # locked_until leads because ix_outbox_reclaim is ordered by it, so the longest
        # abandoned row is recovered first and the scan stays off the published rows.
        .order_by(Outbox.locked_until)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    reclaim = (
        update(Outbox)
        .where(Outbox.id.in_(abandoned))
        .values(
            status=OutboxStatus.PENDING,
            locked_by=None,
            locked_until=None,
            # Due at once, not backed off: an expired lease says the relay stopped, not
            # that the broker refused. Backoff belongs to a rejected delivery (plan 3).
            # One batch shares one now(), so the claim's tie-break falls to id -- and
            # being UUIDv7 that is write order, which is what keeps I5 intact here.
            next_attempt_at=func.now(),
            # attempts is deliberately untouched: the next claim increments it, and
            # incrementing here as well would count a single delivery attempt twice.
        )
        .returning(Outbox.id)
        .execution_options(synchronize_session=False)
    )
    with session.begin():
        reclaimed = len(session.execute(reclaim).scalars().all())
    if reclaimed:
        # Warning, not info: a healthy relay never reclaims anything. Every one of
        # these is a process that died mid-cycle or a publish that outran its lease.
        log.warning("outbox_reclaimed", reclaimed_by=instance_id, count=reclaimed)
    return reclaimed


# 2**32 seconds is 136 years, so clamping here cannot shorten a delay the cap would
# have allowed. It exists because attempts is unbounded -- a retriable failure retries
# forever -- and 1.0 * 2**2000 raises OverflowError rather than returning inf.
_BACKOFF_EXPONENT_LIMIT = 32


def retry_ceiling_seconds(attempts: int, *, base_seconds: float, cap_seconds: float) -> float:
    """The longest a row may wait before its next attempt (spec 7.1: 1 s, x2, cap 60 s).

    Deterministic, and separate from the jitter that draws against it, so the policy
    can be asserted exactly instead of sampled. A claimed row always has ``attempts``
    of at least 1; the floor below is there so the function is total.
    """
    exponent = min(max(attempts - 1, 0), _BACKOFF_EXPONENT_LIMIT)
    return min(cap_seconds, base_seconds * 2**exponent)


def retry_delay(attempts: int, *, base_seconds: float, cap_seconds: float) -> timedelta:
    """How long this row actually waits: full jitter over the ceiling.

    ``uniform(0, ceiling)`` rather than equal or decorrelated jitter, because AWS's
    measurements put full jitter ahead of both on total work and on completion time
    (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).

    The documented objection is that the delay can collapse toward zero, so this
    guarantees no minimum wait. That costs nothing here, and not by luck: librdkafka
    reports ``_MSG_TIMED_OUT`` only once ``delivery.timeout.ms`` has fully elapsed, so
    a retriable failure has *by definition* already spent its whole delivery window.
    The delivery budget paces a relay through an outage whatever the jitter draws.
    """
    ceiling = retry_ceiling_seconds(attempts, base_seconds=base_seconds, cap_seconds=cap_seconds)
    # random, not secrets: this is load spreading, not a security decision.
    return timedelta(seconds=random.uniform(0, ceiling))


def reschedule_failed(
    session: Session,
    failures: Sequence[FailedEvent],
    *,
    instance_id: str,
    base_seconds: float,
    cap_seconds: float,
) -> int:
    """Put retriably-failed rows back on the queue, due after a backoff.

    Returns how many actually moved. Spec 7.2: a retriable failure is never
    dead-lettered, however often it has failed. The backlog is the feature.

    One statement per row, unlike every other transition in this file, because each
    row carries its own delay and its own error text. SQLAlchemy's bulk-UPDATE-by-
    primary-key form would collapse them into one executemany, and it is the wrong
    trade here: it cannot carry RETURNING ("does not support RETURNING because the SQL
    UPDATE statement is executed via DBAPI executemany"), and RETURNING is what counts
    the rows that actually moved. That count is not decoration -- it is how the
    ``locked_by`` guard below reports that another relay took the row. The cost is
    bounded: at most ``BATCH_SIZE`` rows, only on a cycle where a publish was already
    rejected, and one transaction still covers the lot.

    The ``locked_by`` guard is spelled out here rather than shared with
    ``mark_published``, matching how that function spells out its own. It is also what
    makes reading ``attempts`` before writing safe: if the lease has since passed to
    another relay the update matches nothing, which is the correct outcome.
    """
    if not failures:
        return 0
    event_ids = [failure.event_id for failure in failures]
    with session.begin():
        attempts_by_id = {
            row.id: row.attempts
            for row in session.execute(
                select(Outbox.id, Outbox.attempts).where(Outbox.id.in_(event_ids))
            ).all()
        }
        rescheduled = 0
        for failure in failures:
            delay = retry_delay(
                attempts_by_id[failure.event_id],
                base_seconds=base_seconds,
                cap_seconds=cap_seconds,
            )
            reschedule = (
                update(Outbox)
                .where(
                    Outbox.id == failure.event_id,
                    Outbox.status == OutboxStatus.PUBLISHING,
                    Outbox.locked_by == instance_id,
                )
                .values(
                    status=OutboxStatus.PENDING,
                    next_attempt_at=func.now() + delay,
                    last_error=failure.error,
                    locked_by=None,
                    locked_until=None,
                )
                .returning(Outbox.id)
                .execution_options(synchronize_session=False)
            )
            rescheduled += len(session.execute(reschedule).scalars().all())
    if rescheduled:
        # Warning rather than info: a healthy relay reschedules nothing. Each of these
        # is an event the broker refused and this relay has agreed to try again.
        log.warning("outbox_rescheduled", locked_by=instance_id, count=rescheduled)
    return rescheduled


def mark_dead(session: Session, failures: Sequence[FailedEvent], *, instance_id: str) -> int:
    """Park rows that will never publish. Returns how many were parked.

    Spec 7.2: a non-retriable failure is resolved at once rather than retried, because
    a poison message teaches nothing on the thousandth attempt. Nothing is lost --
    ``dead`` is a parking state, not a delete. The row keeps its payload and gains
    ``last_error``, so it stays queryable and replayable where it sits.

    What makes the state worth having is that it is outside both other scans:
    ``claim_batch`` takes only ``pending`` and ``reclaim_expired`` only ``publishing``.
    A dead row is therefore never picked up again, which is the loop this ends.

    Per row for the same reason as ``reschedule_failed``: ``last_error`` differs, and
    the executemany form that would batch them cannot return the count the
    ``locked_by`` guard needs.
    """
    if not failures:
        return 0
    with session.begin():
        parked = 0
        for failure in failures:
            kill = (
                update(Outbox)
                .where(
                    Outbox.id == failure.event_id,
                    Outbox.status == OutboxStatus.PUBLISHING,
                    Outbox.locked_by == instance_id,
                )
                .values(
                    status=OutboxStatus.DEAD,
                    last_error=failure.error,
                    locked_by=None,
                    locked_until=None,
                )
                .returning(Outbox.id)
                .execution_options(synchronize_session=False)
            )
            parked += len(session.execute(kill).scalars().all())
    if parked:
        # error, not warning: a reschedule resolves itself in time, this one needs a
        # person. The event will not reach the topic until someone acts on it.
        log.error("outbox_dead", locked_by=instance_id, count=parked)
    return parked

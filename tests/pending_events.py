"""Outbox rows for the relay's tests, committed the way the api would have."""

from datetime import timedelta
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from outboxexpress.shared.models import AGGREGATE_TYPE_ORDER, Outbox, new_id, utcnow


def write_pending_events(
    sessions: sessionmaker[Session], *, count: int, aggregate_id: str | None = None
) -> list[UUID]:
    """Commit ``count`` pending outbox rows and return their ids in write order.

    Timestamps are spaced into the *past*: a row stamped even a millisecond ahead of
    the server's ``now()`` is not yet due, and the claim would flakily skip it.
    """
    now = utcnow()
    rows = [
        Outbox(
            id=new_id(),
            aggregate_type=AGGREGATE_TYPE_ORDER,
            aggregate_id=aggregate_id or str(new_id()),
            event_type="OrderCreated",
            payload={"sequence": index},
            created_at=now - timedelta(milliseconds=count - index),
            next_attempt_at=now - timedelta(milliseconds=count - index),
        )
        for index in range(count)
    ]
    # Read the ids before the commit, not after. The relay's sessionmaker expires
    # instances on commit -- it has no reason not to -- and a detached instance cannot
    # refresh. The ids are known up front anyway: new_id() generates them client-side.
    event_ids = [row.id for row in rows]
    with sessions() as session, session.begin():
        session.add_all(rows)
    return event_ids

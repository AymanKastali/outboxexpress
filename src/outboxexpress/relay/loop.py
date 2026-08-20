"""One cycle: claim, publish, mark. Two short transactions with the network between.

Read top to bottom, this file is the whole relay path in spec 6.2. The lock is
released before the produce call, which is the entire reason there are two
transactions rather than one: a wedged broker never holds a Postgres row or a pooled
connection hostage.
"""

import socket
import uuid

from confluent_kafka import Producer
from sqlalchemy.orm import Session

from outboxexpress.shared.config import Settings
from outboxexpress.shared.logging import get_logger

from . import hooks
from .outbox import FailedEvent, claim_batch, mark_dead, mark_published, reschedule_failed
from .publisher import flush_timeout_seconds, is_retriable, publish_batch

log = get_logger(__name__)


def new_instance_id() -> str:
    """Hostname plus a per-process id, so restarts of one container are distinguishable.

    Nothing about correctness depends on the value -- only on the lease (spec 6.2). It
    exists for diagnosis and for I3.
    """
    return f"{socket.gethostname()}-{uuid.uuid4()}"


def run_once(
    session: Session, producer: Producer, *, instance_id: str, settings: Settings
) -> int:
    """Publish one batch. Returns how many rows were retired.

    A full batch means there is probably more waiting, so the caller can loop again
    immediately rather than sleeping -- which is why this returns a count and not None.
    """
    claimed = claim_batch(
        session,
        instance_id=instance_id,
        batch_size=settings.relay_batch_size,
        lease_seconds=settings.relay_lease_seconds,
    )
    if not claimed:
        return 0
    hooks.after_claim()

    outcomes = publish_batch(
        producer,
        claimed,
        topic=settings.kafka_topic,
        timeout_seconds=flush_timeout_seconds(settings),
    )
    hooks.after_publish_before_mark()

    delivered: list[uuid.UUID] = []
    retriable: list[FailedEvent] = []
    poisoned: list[FailedEvent] = []
    for outcome in outcomes:
        if outcome.error is None:
            delivered.append(outcome.event_id)
        elif is_retriable(outcome.error):
            retriable.append(FailedEvent(event_id=outcome.event_id, error=str(outcome.error)))
        else:
            poisoned.append(FailedEvent(event_id=outcome.event_id, error=str(outcome.error)))

    marked = mark_published(session, delivered, instance_id=instance_id)
    hooks.after_mark()
    rescheduled = reschedule_failed(
        session,
        retriable,
        instance_id=instance_id,
        base_seconds=settings.relay_backoff_base_seconds,
        cap_seconds=settings.relay_backoff_cap_seconds,
    )
    parked = mark_dead(session, poisoned, instance_id=instance_id)

    log.info(
        "relay_cycle",
        locked_by=instance_id,
        claimed=len(claimed),
        published=marked,
        rescheduled=rescheduled,
        dead=parked,
        # Claimed but never reported on, so nothing here can classify it. These rows
        # keep their lease and the reclaimer is what moves them -- the only job it has
        # left now that a verdict is resolved in the cycle that produced it.
        unreported=len(claimed) - len(outcomes),
    )
    return marked

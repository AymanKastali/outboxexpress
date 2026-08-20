"""The producer. Knows about Kafka and nothing about Postgres."""

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from outboxexpress.shared.config import Settings
from outboxexpress.shared.logging import get_logger

from .outbox import ClaimedEvent

log = get_logger(__name__)

# Spec 7.2's poison list: the errors where the *message* is the problem. Everything
# absent from it is a property of the environment -- a broker down, a topic missing, an
# ACL not yet granted -- and retries, because someone can fix that and the outbox is
# what makes waiting survivable.
#
# A denylist because this side of the line is closed and the other is open-ended. No
# payload gets smaller by waiting; almost anything else might.
NON_RETRIABLE_CODES = frozenset(
    {
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.RECORD_LIST_TOO_LARGE,
        KafkaError.INVALID_RECORD,
        # CORRUPT_MESSAGE under its librdkafka name. Kafka's protocol table calls this
        # one retriable, which is the broker's view of a re-read; a record we produced
        # corrupt does not arrive intact on resend, so librdkafka's "permanent" wins.
        KafkaError.INVALID_MSG,
        # Only a serializing producer reports these two, and this relay encodes its
        # own payloads from JSONB that Postgres already validated. Listed because spec
        # 7.2 names serialisation failure as poison, so this is the spec's table
        # rather than a subset of it.
        KafkaError._KEY_SERIALIZATION,
        KafkaError._VALUE_SERIALIZATION,
    }
)


def is_retriable(error: KafkaError) -> bool:
    """Whether this failure is worth waiting out rather than giving up on (spec 7.2).

    This is a *second-tier* judgement. librdkafka has already retried the retriable
    errors internally until ``delivery.timeout.ms`` expired, then reported
    ``_MSG_TIMED_OUT`` -- an umbrella code that abstracts away whichever broker error
    it kept hitting. So a retriable verdict here means a full delivery window was
    already spent failing, and the question is only whether to spend another one later.

    This deliberately calls ``UNKNOWN_TOPIC_OR_PART`` and the authorisation failures
    retriable, where librdkafka calls them permanent. Both are right at their own
    horizon: 30 seconds will not conjure an ACL, and a day very well might. Dead-
    lettering a backlog over a mistyped topic name is the failure spec 7.2 opens with.

    Deliberately not ``error.retriable()``. That flag is the *transactional*
    producer's categorisation -- every documented use of it sits inside a
    ``commit_transaction`` retry loop -- and it is not set on delivery reports.
    Measured on confluent-kafka 2.15.0: a real report from an unreachable broker
    carries ``_MSG_TIMED_OUT`` with ``retriable()`` False, and a ``NOT_ENOUGH_REPLICAS``
    reports False too. Classifying on it would dead-letter every broker outage.
    """
    return error.code() not in NON_RETRIABLE_CODES


@dataclass(frozen=True)
class PublishOutcome:
    """What the broker said about one event. ``error is None`` means delivered."""

    event_id: UUID
    error: KafkaError | None


def producer_config(settings: Settings) -> dict[str, Any]:
    """Spec 8.2 verbatim.

    ``enable.idempotence`` buys deduplication of librdkafka's *own* retries within a
    session -- worth having, and not to be confused with closing the ack-before-mark
    window in 6.2, which no producer setting can close.
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 5,
        "delivery.timeout.ms": settings.kafka_delivery_timeout_ms,
    }


def create_producer(settings: Settings) -> Producer:
    return Producer(producer_config(settings))


def flush_timeout_seconds(settings: Settings) -> float:
    """How long to wait for every delivery report the batch is owed.

    librdkafka fails a message of its own accord once ``delivery.timeout.ms`` passes,
    so waiting past that adds only the margin needed for the last report to surface.
    Giving up sooner would discard verdicts that were about to arrive.
    """
    return settings.kafka_delivery_timeout_ms / 1000 + 1


def publish_batch(
    producer: Producer,
    events: Sequence[ClaimedEvent],
    *,
    topic: str,
    timeout_seconds: float,
) -> list[PublishOutcome]:
    """Produce the batch and return the broker's verdict on each event.

    An event with no verdict by ``timeout_seconds`` is left out of the result rather
    than guessed at. The caller marks only what is reported delivered, so an omitted
    event keeps its lease and is retried after it expires.

    Guarantees on return *and* on raising: nothing this call queued is still pending.
    """
    # Unsynchronised on purpose: confluent-kafka serves delivery callbacks from the
    # thread that calls poll() or flush(), never from librdkafka's background thread,
    # so every write below happens inside the flush() call further down.
    verdicts: dict[UUID, KafkaError | None] = {}

    # No poll(0) between produces, and no BufferError guard: the documented reasons for
    # both are a producer queue filling up, and BATCH_SIZE is 100 against librdkafka's
    # default queue.buffering.max.messages of 100000. Raise the batch by three orders of
    # magnitude and this loop needs the guard back.
    try:
        for event in events:
            try:
                producer.produce(
                    topic,
                    key=event.aggregate_id,
                    value=json.dumps(event.payload).encode(),
                    headers=[
                        ("event_id", str(event.event_id)),
                        ("event_type", event.event_type),
                    ],
                    on_delivery=_report_into(verdicts, event.event_id),
                )
            except KafkaException as rejected:
                # produce() applies some checks locally and synchronously -- the message
                # size is the documented one -- and raises instead of reporting.
                # Recording the verdict here is what lets the caller resolve this one
                # event and keep producing the rest of the batch.
                error: KafkaError = rejected.args[0]
                verdicts[event.event_id] = error
                log.warning("publish_rejected", event_id=str(event.event_id), error=str(error))
    finally:
        # Whatever else escapes this loop -- a BufferError from a queue that filled
        # against expectations -- must not leave queued messages to drain at some
        # arbitrary later moment while the reclaimer is republishing the same rows.
        # flush() returns the count still queued: those are the events with no verdict.
        undelivered = producer.flush(timeout_seconds)
        if undelivered:
            log.warning("publish_unreported", topic=topic, count=undelivered)

    return [
        PublishOutcome(event_id=event.event_id, error=verdicts[event.event_id])
        for event in events
        if event.event_id in verdicts
    ]


def _report_into(
    verdicts: dict[UUID, KafkaError | None], event_id: UUID
) -> Callable[[KafkaError | None, Message], None]:
    """Build the delivery callback for one event, writing its verdict into ``verdicts``.

    Closing over the loop variable instead would file every verdict against the last
    event, so the id is captured as an argument here.
    """

    def delivered(error: KafkaError | None, _message: Message) -> None:
        verdicts[event_id] = error
        if error is not None:
            log.warning("publish_failed", event_id=str(event_id), error=str(error))

    return delivered

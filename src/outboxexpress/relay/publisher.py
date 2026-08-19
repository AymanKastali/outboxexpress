"""The producer. Knows about Kafka and nothing about Postgres."""

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from confluent_kafka import KafkaError, Message, Producer

from outboxexpress.shared.config import Settings
from outboxexpress.shared.logging import get_logger

from .outbox import ClaimedEvent

log = get_logger(__name__)


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
    finally:
        # produce() rejects some messages locally rather than through the callback -- an
        # oversized payload, an invalid argument -- and that raises mid-loop. Flushing
        # here delivers and reports whatever was already queued before the error leaves
        # this function, rather than letting it drain at some arbitrary later moment
        # while the reclaimer is republishing the same rows. flush() returns the count
        # still queued: those are the events that end up with no verdict.
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

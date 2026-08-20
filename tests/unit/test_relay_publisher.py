"""The producer half of the cycle. No Postgres in this file."""

import json

from confluent_kafka import KafkaError, Message

from outboxexpress.relay.outbox import ClaimedEvent
from outboxexpress.relay.publisher import (
    create_producer,
    flush_timeout_seconds,
    is_retriable,
    producer_config,
    publish_batch,
)
from outboxexpress.shared.config import Settings
from outboxexpress.shared.models import new_id
from topic_reader import read_events


def header(message: Message, name: str) -> bytes:
    """One header value, narrowed to bytes.

    confluent-kafka types headers as either a dict or a list of pairs, with each value
    ``str | bytes | None``, because librdkafka permits all of those. A consumed message
    always carries the list form with bytes values, so the asserts narrow to that.
    """
    headers = message.headers()
    assert isinstance(headers, list)
    value = dict(headers)[name]
    assert isinstance(value, bytes)
    return value


def an_event(aggregate_id: str, sequence: int) -> ClaimedEvent:
    return ClaimedEvent(
        event_id=new_id(),
        aggregate_id=aggregate_id,
        event_type="OrderCreated",
        payload={"sequence": sequence},
    )


def test_the_producer_config_is_the_one_the_spec_fixes() -> None:
    config = producer_config(Settings(kafka_bootstrap_servers="host:9092"))
    assert config["bootstrap.servers"] == "host:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["compression.type"] == "lz4"
    assert config["linger.ms"] == 5
    assert config["delivery.timeout.ms"] == 30_000


def test_the_flush_timeout_outlasts_the_delivery_budget() -> None:
    # A flush that gave up before delivery.timeout.ms elapsed would report no verdict
    # for a message the broker was still going to accept.
    settings = Settings(kafka_delivery_timeout_ms=30_000, relay_lease_seconds=60)
    assert flush_timeout_seconds(settings) > settings.kafka_delivery_timeout_ms / 1000


def test_an_event_arrives_keyed_by_its_aggregate_with_its_id_in_the_header(
    broker: str, topic: str
) -> None:
    event = an_event("order-7", 0)
    settings = Settings(kafka_bootstrap_servers=broker)
    producer = create_producer(settings)

    outcomes = publish_batch(
        producer, [event], topic=topic, timeout_seconds=flush_timeout_seconds(settings)
    )

    assert [(o.event_id, o.error) for o in outcomes] == [(event.event_id, None)]
    (message,) = read_events(broker, topic, expected=1)
    assert message.key() == b"order-7"
    value = message.value()
    assert value is not None
    assert json.loads(value) == {"sequence": 0}
    assert header(message, "event_id") == str(event.event_id).encode()
    assert header(message, "event_type") == b"OrderCreated"


def test_one_aggregate_lands_on_one_partition_in_produce_order(broker: str, topic: str) -> None:
    # I5. The key puts them on one partition; enable.idempotence is what stops
    # librdkafka's own retries from reordering them once they are there.
    events = [an_event("order-7", sequence) for sequence in range(5)]
    settings = Settings(kafka_bootstrap_servers=broker)
    producer = create_producer(settings)

    publish_batch(producer, events, topic=topic, timeout_seconds=flush_timeout_seconds(settings))

    messages = read_events(broker, topic, expected=5)
    assert len(messages) == 5
    assert len({message.partition() for message in messages}) == 1
    arrived = [header(message, "event_id").decode() for message in messages]
    assert arrived == [str(event.event_id) for event in events]


def test_an_unreachable_broker_reports_failure_rather_than_raising() -> None:
    # No broker fixture and no real topic: the point is that produce() queues the
    # message and the verdict arrives later, through the delivery report.
    settings = Settings(
        kafka_bootstrap_servers="127.0.0.1:1",
        kafka_delivery_timeout_ms=2_000,
        relay_lease_seconds=60,
    )
    producer = create_producer(settings)

    outcomes = publish_batch(
        producer,
        [an_event("order-7", 0)],
        topic="orders.events",
        timeout_seconds=flush_timeout_seconds(settings),
    )

    assert len(outcomes) == 1
    assert outcomes[0].error is not None


def test_a_broker_outage_is_retriable() -> None:
    # The code a real delivery report actually carries when the broker is unreachable.
    # Classifying this one as poison would dead-letter an entire outage, which is the
    # single outcome spec 7.2 exists to prevent.
    assert is_retriable(KafkaError(KafkaError._MSG_TIMED_OUT))
    assert is_retriable(KafkaError(KafkaError._TRANSPORT))
    assert is_retriable(KafkaError(KafkaError.NOT_ENOUGH_REPLICAS))
    assert is_retriable(KafkaError(KafkaError.LEADER_NOT_AVAILABLE))
    assert is_retriable(KafkaError(KafkaError.REQUEST_TIMED_OUT))


def test_a_message_that_is_itself_the_problem_is_poison() -> None:
    # No payload gets smaller, or less corrupt, by being retried.
    assert not is_retriable(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
    assert not is_retriable(KafkaError(KafkaError.RECORD_LIST_TOO_LARGE))
    assert not is_retriable(KafkaError(KafkaError.INVALID_RECORD))
    # CORRUPT_MESSAGE. Kafka's protocol table marks this retriable, describing a
    # broker re-reading a request; a record produced corrupt stays corrupt.
    assert not is_retriable(KafkaError(KafkaError.INVALID_MSG))


def test_a_fixable_environment_is_retriable_even_where_librdkafka_says_permanent() -> None:
    # The deliberate departure, pinned so it reads as a decision and not an oversight.
    # librdkafka calls these permanent, correctly, for its own 30-second horizon. The
    # outbox's horizon is days, and a missing grant or a mistyped topic name is
    # something a person fixes inside days. Dead-lettering a whole backlog over a
    # config typo is the failure spec 7.2 opens by naming.
    assert is_retriable(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
    assert is_retriable(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    assert is_retriable(KafkaError(KafkaError.CLUSTER_AUTHORIZATION_FAILED))


def test_an_unrecognised_error_is_retriable() -> None:
    # The denylist's whole point: a code nobody classified waits rather than dies,
    # because the poison side of the line is closed and this side is not.
    assert is_retriable(KafkaError(KafkaError.UNKNOWN))
    assert is_retriable(KafkaError(KafkaError.INVALID_REQUIRED_ACKS))


def test_classification_does_not_use_librdkafkas_transactional_flag() -> None:
    """Pins the finding in spec 7.2, so a later simplification fails here instead.

    ``KafkaError.retriable()`` is the transactional API's categorisation and is not
    set on delivery reports. Swapping ``is_retriable`` for it looks like a tidy-up
    and would dead-letter every broker outage.
    """
    outage = KafkaError(KafkaError.NOT_ENOUGH_REPLICAS)
    assert outage.retriable() is False
    assert is_retriable(outage) is True


def test_a_locally_rejected_event_gets_a_verdict_and_its_siblings_still_publish(
    broker: str, topic: str
) -> None:
    # produce() checks the message size itself and raises rather than reporting, so
    # without a verdict this event would leave publish_batch as an exception and the
    # row would be reclaimed and retried for as long as the payload stays too big.
    # librdkafka's default message.max.bytes is 1000000.
    oversized = ClaimedEvent(
        event_id=new_id(),
        aggregate_id="order-7",
        event_type="OrderCreated",
        payload={"filler": "x" * 2_000_000},
    )
    good = an_event("order-8", 0)
    settings = Settings(kafka_bootstrap_servers=broker)

    outcomes = publish_batch(
        create_producer(settings),
        [oversized, good],
        topic=topic,
        timeout_seconds=flush_timeout_seconds(settings),
    )

    verdicts = {outcome.event_id: outcome.error for outcome in outcomes}
    rejected = verdicts[oversized.event_id]
    assert rejected is not None
    assert rejected.code() == KafkaError.MSG_SIZE_TOO_LARGE
    assert not is_retriable(rejected)
    # The sibling is the point: one poison payload must not cost the other ninety-nine.
    assert verdicts[good.event_id] is None

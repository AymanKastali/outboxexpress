"""One full cycle, including the crash windows spec 6.2 says cannot be closed."""

from collections.abc import Sequence

import pytest
from confluent_kafka import KafkaError, Producer
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outbox_state import statuses
from outboxexpress.relay import hooks, loop
from outboxexpress.relay.outbox import ClaimedEvent
from outboxexpress.relay.publisher import PublishOutcome, create_producer
from outboxexpress.shared.config import Settings
from pending_events import write_pending_events
from topic_reader import QUIET_TIMEOUT, read_events


class Boom(Exception):
    """Stands in for the relay process dying at a named seam."""


def test_a_cycle_publishes_every_claimed_row_and_retires_it(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    broker: str,
) -> None:
    write_pending_events(relay_sessions, count=3)
    producer = create_producer(relay_settings)

    with relay_sessions() as session:
        marked = loop.run_once(session, producer, instance_id="relay-1", settings=relay_settings)

    assert marked == 3
    assert statuses(sync_engine) == ["published"] * 3
    assert len(read_events(broker, relay_settings.kafka_topic, expected=3)) == 3


def test_an_empty_outbox_retires_nothing(
    relay_sessions: sessionmaker[Session], relay_settings: Settings
) -> None:
    producer = create_producer(relay_settings)

    with relay_sessions() as session:
        assert loop.run_once(session, producer, instance_id="relay-1", settings=relay_settings) == 0


def test_a_crash_between_claim_and_publish_leaves_the_row_claimed(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    broker: str,
) -> None:
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_claim", _raise)
    producer = create_producer(relay_settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=relay_settings)

    # Nothing published, and the row still holds its lease. Recovering it is the
    # reclaimer's job in plan 2 -- until then this is where the row stops.
    assert statuses(sync_engine) == ["publishing"]
    assert read_events(broker, relay_settings.kafka_topic, expected=1, timeout=QUIET_TIMEOUT) == []


def test_a_crash_after_the_ack_leaves_kafka_ahead_of_postgres(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    broker: str,
) -> None:
    # Spec 6.2's one irreducible window, asserted rather than hoped for: the event is
    # on the topic and the row still says publishing. The lease will expire, the row
    # will be republished, and the consumer's dedup absorbs it. This is why the
    # guarantee is at-least-once.
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_publish_before_mark", _raise)
    producer = create_producer(relay_settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=relay_settings)

    assert statuses(sync_engine) == ["publishing"]
    assert len(read_events(broker, relay_settings.kafka_topic, expected=1)) == 1


def test_the_mark_hook_runs_after_the_row_is_retired(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_mark", _raise)
    producer = create_producer(relay_settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=relay_settings)

    assert statuses(sync_engine) == ["published"]


def test_a_cycle_retires_only_the_events_the_broker_confirmed(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The property the whole design rests on: `published` implies Kafka acked it.

    Every other test in this suite still passes if run_once stops filtering on the
    delivery verdict, because no other test produces a half-failed batch. This one
    does. No broker is needed -- publish_batch is the thing being replaced.
    """
    write_pending_events(relay_sessions, count=2)
    settings = Settings(kafka_bootstrap_servers="127.0.0.1:1", kafka_topic="orders.events")

    def first_delivered_second_rejected(
        _producer: Producer,
        events: Sequence[ClaimedEvent],
        *,
        topic: str,
        timeout_seconds: float,
    ) -> list[PublishOutcome]:
        return [
            PublishOutcome(event_id=events[0].event_id, error=None),
            PublishOutcome(
                event_id=events[1].event_id, error=KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
            ),
        ]

    monkeypatch.setattr(loop, "publish_batch", first_delivered_second_rejected)

    with relay_sessions() as session:
        marked = loop.run_once(
            session, create_producer(settings), instance_id="relay-1", settings=settings
        )

    # The rejected row is resolved in this same cycle rather than left for the
    # reclaimer: MSG_SIZE_TOO_LARGE is poison, so it is parked. What must never
    # happen is it being retired alongside its sibling.
    assert marked == 1
    assert statuses(sync_engine) == ["published", "dead"]


def test_a_retriable_rejection_returns_the_row_to_pending_with_its_error(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A broker outage, in the shape the relay actually sees it: a verdict carrying
    # _MSG_TIMED_OUT. The row must come back to pending -- never dead, however many
    # times this happens (spec 7.2).
    write_pending_events(relay_sessions, count=1)

    def timed_out(
        _producer: Producer,
        events: Sequence[ClaimedEvent],
        *,
        topic: str,
        timeout_seconds: float,
    ) -> list[PublishOutcome]:
        return [
            PublishOutcome(event_id=events[0].event_id, error=KafkaError(KafkaError._MSG_TIMED_OUT))
        ]

    monkeypatch.setattr(loop, "publish_batch", timed_out)

    with relay_sessions() as session:
        marked = loop.run_once(
            session,
            create_producer(relay_settings),
            instance_id="relay-1",
            settings=relay_settings,
        )

    assert marked == 0
    assert statuses(sync_engine) == ["pending"]
    with sync_engine.connect() as connection:
        assert connection.scalar(text("SELECT last_error FROM outbox")) is not None


def test_an_event_with_no_verdict_keeps_its_lease_for_the_reclaimer(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The reclaimer's remaining job after this plan, pinned. No verdict means neither
    # delivered nor rejected, so there is nothing to classify and nothing to resolve;
    # the lease expiring is the only thing that can move this row.
    write_pending_events(relay_sessions, count=1)

    def no_report(
        _producer: Producer,
        events: Sequence[ClaimedEvent],
        *,
        topic: str,
        timeout_seconds: float,
    ) -> list[PublishOutcome]:
        return []

    monkeypatch.setattr(loop, "publish_batch", no_report)

    with relay_sessions() as session:
        loop.run_once(
            session,
            create_producer(relay_settings),
            instance_id="relay-1",
            settings=relay_settings,
        )

    assert statuses(sync_engine) == ["publishing"]


def test_instance_ids_distinguish_two_processes_on_one_host() -> None:
    assert loop.new_instance_id() != loop.new_instance_id()


def _raise() -> None:
    raise Boom

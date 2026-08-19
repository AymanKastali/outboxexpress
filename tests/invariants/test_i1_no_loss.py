"""I1: every committed order reaches the topic, whatever the relay does mid-cycle.

The mechanism is the reclaimer. Each case kills the relay at one of spec 9.1's three
seams, then starts a second relay under a new instance id and asserts the event still
arrives -- because a reclaimed row that only its original owner could publish would
recover nothing.
"""

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from outbox_state import expire_leases, statuses
from outboxexpress.relay import hooks, loop
from outboxexpress.relay.outbox import reclaim_expired
from outboxexpress.relay.publisher import create_producer
from outboxexpress.shared.config import Settings
from pending_events import write_pending_events
from topic_reader import read_events

SEAMS = ["after_claim", "after_publish_before_mark", "after_mark"]


class Boom(Exception):
    """Stands in for the relay process dying at a named seam."""


@pytest.mark.parametrize("seam", SEAMS)
def test_a_crash_at_any_seam_still_delivers_the_event(
    seam: str,
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    relay_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    broker: str,
) -> None:
    write_pending_events(relay_sessions, count=1)

    # The context manager, not a bare setattr plus undo(): pytest's own docs say there is
    # generally no need to call undo() and point at context() for exactly this -- a patch
    # that has to come off before the test ends. The block is the relay that dies.
    with monkeypatch.context() as crash:
        crash.setattr(hooks, seam, _raise)
        with relay_sessions() as session, pytest.raises(Boom):
            loop.run_once(
                session,
                create_producer(relay_settings),
                instance_id="relay-1",
                settings=relay_settings,
            )

    # The replacement relay, with the fault gone and a fresh producer -- a restart, not a
    # retry. A new instance id on purpose: the row must be claimable by a relay other
    # than the one that abandoned it.
    expire_leases(sync_engine)
    with relay_sessions() as session:
        reclaim_expired(
            session, instance_id="relay-2", batch_size=relay_settings.relay_batch_size
        )
        loop.run_once(
            session,
            create_producer(relay_settings),
            instance_id="relay-2",
            settings=relay_settings,
        )

    # `expected=1` reads "at least the one", which is exactly what no-loss claims. The
    # after_publish_before_mark seam genuinely puts a second copy on the topic; that
    # window is spec 6.2's irreducible one, and absorbing it belongs to I4 in M3.
    assert len(read_events(broker, relay_settings.kafka_topic, expected=1)) == 1
    assert statuses(sync_engine) == ["published"]


def _raise() -> None:
    raise Boom

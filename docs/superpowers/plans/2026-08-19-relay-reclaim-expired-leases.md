# Relay Lease Reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Return an abandoned `publishing` row to `pending` once its lease has expired, so no claimed event is ever lost.

**Architecture:** One new statement in `relay/outbox.py`, alongside the claim and the mark it completes. The reclaim is the mirror image of the claim — same `FOR UPDATE SKIP LOCKED` over the same table, same single short transaction — because it exists for the same reason: with N relays scanning concurrently, two of them must never take one row.

Nothing calls it yet. Plan 1 shipped `run_once` with no caller and proved it with `make test`; the reclaim ships the same way, because the thing that *schedules* it is the daemon loop, and that belongs to plan 4. What this plan delivers is the recovery itself, and the two invariants that recovery is what makes true: **I1** (no loss) and **I3** (no double-claim).

**Tech Stack:** Python 3.14 · SQLAlchemy 2.0 (sync Core statements, explicit `session.begin()`) · PostgreSQL 18 `FOR UPDATE SKIP LOCKED` · pytest + testcontainers (Postgres 18, `apache/kafka-native:4.3.1`) · `confluent-kafka` 2.15.0

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§5.2 `ix_outbox_reclaim`, §6.2 the reclaimer, §7.1 timings, §9.1 hooks, §9.2 I1 and I3)

## Global Constraints

- Python `==3.14.*`; every dependency pinned exactly. **This plan adds no dependency at all** — every import it needs is already installed.
- Relay state transitions live in `relay/outbox.py` (spec §10). The reclaim does not get its own module.
- The reclaim query is fixed by spec §6.2: `status='publishing' AND locked_until < now()`, `FOR UPDATE SKIP LOCKED`, resetting `status` to `pending`, clearing `locked_by` and `locked_until`, and setting `next_attempt_at = now()`.
- `ix_outbox_reclaim` already exists — `(locked_until) WHERE status = 'publishing'` (§5.2). The query must be orderable by `locked_until` so it uses that index rather than scanning.
- `settings` env prefix stays `OUTBOX_`; `ruff` line-length 100; `pyright` strict over `src`, `tests`, `migrations`.
- `relay` imports `shared` only. It must not import `api` or `notifier` (§4.1).
- Test helpers that are plain functions go in a sibling module under `tests/`; only fixtures go in `conftest.py` (§9.5).

## Out of Scope

- **`RECLAIM_INTERVAL` and anything that schedules the reclaim** (plan 4). The interval is a property of the daemon loop, and adding the setting now would ship configuration nothing reads. Consequence, stated rather than hidden: after this plan a stranded row is recoverable but is not yet recovered automatically — the tests are what call `reclaim_expired`.
- **Error classification, backoff, and the DLQ** (plan 3). A reclaimed row is due *immediately*, with no backoff and no `last_error`, because nothing about an expired lease says the publish failed — only that the relay holding it stopped. Backoff belongs to a delivery that was actually rejected.
- **I4 (duplicates tolerated) and I5 (per-order ordering).** I4 needs the notifier's dedup to count emails, so it lands in M3. I5 belongs with plan 3's retry paths, where reordering would actually become possible.
- **`relay/__main__.py`, signal handling, the compose service, chaos targets** (plan 4).

## Plan Queue

M2 is split across small plans. Plan 1 has shipped; **this is plan 2**.

1. ~~Claim, publish, mark — one relay cycle~~ — `2026-08-19-relay-publish-cycle.md` (shipped, PR #4)
2. **[this plan]** Reclaim expired leases — proves I1 and I3 — `2026-08-19-relay-reclaim-expired-leases.md`
3. Error classification, backoff, and DLQ routing — write after plan 2 ships
4. Kafka and the relay under compose, with chaos targets — write after plan 3 ships

## Spec Amendments (already applied)

The spec was updated alongside this plan, so it does not drift during execution. No task needs to touch it:

- **§9.2** — I3's "how it is forced" now names both races the invariant covers: two relays claiming at once, and two reclaiming at once. The second is the one that matters for correctness after this plan, because a row reclaimed twice is a row published twice for no reason but the recovery racing itself.
- **§6.2** — the reclaimer paragraph misattributed its own mutual exclusion to `SKIP LOCKED`. It now names the mechanism that actually provides it (the row lock plus the `status='publishing'` predicate, re-read after a lock wait) and what `SKIP LOCKED` is for (lock contention), both cited to the PostgreSQL docs and both measured on Postgres 18. Nothing about the query changes — only the reason given for it, which is what an implementer would otherwise copy into a comment.
- **§10** — the `tests/` tree gains `outbox_state.py`.

---

### Task 1: Reclaim an expired lease

**Files:**
- Modify: `src/outboxexpress/relay/outbox.py` (add `reclaim_expired` below `mark_published`)
- Create: `tests/outbox_state.py`
- Modify: `tests/unit/test_relay_loop.py:27-31` (import `statuses` instead of defining it)
- Test: `tests/unit/test_relay_outbox.py` (four new tests, and one rename)

`statuses` moves out of `test_relay_loop.py` rather than being copied into the invariant modules that Tasks 2 and 3 add. It is the same question asked of the same table for the same reason, so it is one function; three copies of it would be three places to fix when the ordering changes. `expire_leases` joins it because it answers the neighbouring question — both are how a test observes or nudges outbox row state, and neither is a fixture.

Ageing the lease with `UPDATE` rather than sleeping through one is the repo's existing move: `test_a_row_not_yet_due_is_left_alone` already pushes `next_attempt_at` an hour into the future to test the claim's clock. This is the same trick on the other timestamp. A real sleep would cost a second per test and be flaky at the boundary.

**Interfaces:**
- Consumes: `claim_batch`, `mark_published` (`relay/outbox.py`); `write_pending_events` (`tests/pending_events.py`); the `relay_sessions` and `sync_engine` fixtures.
- Produces: `reclaim_expired(session: Session, *, instance_id: str, batch_size: int) -> int` — how many rows it recovered. `expire_leases(engine: Engine) -> None` and `statuses(engine: Engine) -> list[str]` in `tests/outbox_state.py`.

- [ ] **Step 1: Write the shared test helpers**

Create `tests/outbox_state.py`:

```python
"""Reading and nudging outbox row state, for the relay's tests.

Plain functions rather than fixtures, so they go in a sibling module and are imported
normally -- see spec 9.5.
"""

from sqlalchemy import Engine, text


def statuses(engine: Engine) -> list[str]:
    """Every row's status, in the order the claim would take them."""
    with engine.connect() as connection:
        return list(
            connection.scalars(text("SELECT status FROM outbox ORDER BY next_attempt_at, id"))
        )


def expire_leases(engine: Engine) -> None:
    """Age every held lease past its expiry, as if the relay holding it had stalled.

    Sleeping through a real lease would cost a second per test and be flaky at the
    boundary. Moving the clock instead is what ``test_a_row_not_yet_due_is_left_alone``
    already does to ``next_attempt_at``; this is the same move on ``locked_until``.
    """
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE outbox SET locked_until = now() - interval '1 second' "
                "WHERE status = 'publishing'"
            )
        )
```

Then delete the local `statuses` from `tests/unit/test_relay_loop.py` (lines 27-31) and import it instead, dropping the now-unused `Engine`/`text` imports only if nothing else in that file uses them (`sync_engine: Engine` annotations still do, so `Engine` stays; `text` goes):

```python
from outbox_state import statuses
```

- [ ] **Step 2: Rename the test whose name overstates it**

`test_two_relays_claiming_at_once_get_disjoint_rows` in `tests/unit/test_relay_outbox.py` claims a concurrency it never creates: `claim_batch` commits before returning, so the second call sees the first's rows as `publishing` and simply skips them on status. The behaviour it does pin is worth keeping — a claimed row is not claimed twice — so rename it rather than delete it, and let Task 3 own the concurrent case:

```python
def test_a_claimed_row_is_not_offered_to_the_next_claim(
    relay_sessions: sessionmaker[Session],
) -> None:
    # Sequential on purpose: claim_batch commits before returning, so this pins the
    # status guard, not SKIP LOCKED. Two relays genuinely overlapping is I3's job.
    write_pending_events(relay_sessions, count=6)
```

Keep the body exactly as it is; only the name and that comment change. The stale comment currently pointing at "plan 2" goes with it.

- [ ] **Step 3: Write the failing tests**

Append to `tests/unit/test_relay_outbox.py`, adding `reclaim_expired` to the existing `outboxexpress.relay.outbox` import and `from outbox_state import expire_leases, statuses` alongside it:

```python
def test_an_expired_lease_returns_the_row_to_pending(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=10) == 1

    state = row_state(sync_engine, event_id)
    assert state["status"] == "pending"
    assert state["locked_by"] is None
    assert state["locked_until"] is None
    # The attempt was already counted when the row was claimed. Counting it again here
    # would make `attempts` say two deliveries were tried when only one was.
    assert state["attempts"] == 1


def test_a_live_lease_is_left_alone(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # The lease's whole purpose: a relay still inside its window keeps its row, so a
    # slow publish is never republished underneath the process performing it.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=10) == 0

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_a_reclaim_stops_at_the_batch_size(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    write_pending_events(relay_sessions, count=5)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=5, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=3) == 3

    assert statuses(sync_engine).count("pending") == 3


def test_a_reclaimed_row_is_claimable_by_another_relay(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # Recovery is only worth anything if the row can then be published. This is the
    # join between the reclaim and the claim, and it is what I1 rests on.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    expire_leases(sync_engine)

    with relay_sessions() as session:
        reclaim_expired(session, instance_id="relay-2", batch_size=10)
        claimed = claim_batch(session, instance_id="relay-2", batch_size=10, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == [event_id]
    assert row_state(sync_engine, event_id)["locked_by"] == "relay-2"
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: the four new tests ERROR at collection with `ImportError: cannot import name 'reclaim_expired'`. The existing eight are unaffected.

- [ ] **Step 5: Write the implementation**

Append to `src/outboxexpress/relay/outbox.py`:

```python
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
```

`instance_id` is not written to the row — the reclaim *clears* `locked_by`. It is here for the log line, which is the operational signal that tells you which of three relays is picking up after which, and it keeps the module's three functions one shape.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_outbox.py tests/unit/test_relay_loop.py -v`
Expected: 12 passed in `test_relay_outbox.py`, 7 still passing in `test_relay_loop.py` after the `statuses` move.

- [ ] **Step 7: Lint and type-check**

Run: `uv run ruff check . --fix && uv run pyright`
Expected: no errors. `outbox_state` sorts into the same first-party group as `outboxexpress` because `src = ["src", "tests"]`, and `_` sorts before `e`, so it lands *above* the `outboxexpress` imports. Let `--fix` place it rather than guessing.

- [ ] **Step 8: Commit**

```bash
git add src/outboxexpress/relay/outbox.py tests/outbox_state.py tests/unit/test_relay_outbox.py tests/unit/test_relay_loop.py
git commit -m "feat: return abandoned outbox rows to pending when the lease expires"
```

---

### Task 2: I1 — no loss at any seam

**Files:**
- Create: `tests/invariants/test_i1_no_loss.py`
- Modify: `tests/conftest.py` (add the `relay_settings` fixture)
- Modify: `tests/unit/test_relay_loop.py:22-24` (drop its local `settings` fixture, take `relay_settings`)

The `settings` fixture moves rather than being copied. Spec §9.5 is explicit that fixtures live in `conftest.py` and arrive by injection, and this task is where the second consumer appears — which is the point at which one shared fixture beats two identical local ones. It gains the `relay_` prefix on the way, pairing with the `relay_sessions` fixture already there; at module scope `settings` was clear enough, but in `conftest.py` it would not say whose settings it builds.

**Interfaces:**
- Consumes: `reclaim_expired`, `loop.run_once`, `hooks`, `create_producer`, `write_pending_events`, `read_events`, `expire_leases`, `statuses`; the `broker`, `topic`, `relay_sessions` and `sync_engine` fixtures.
- Produces: `relay_settings` fixture → `Settings` pointed at the test broker and that test's topic.

- [ ] **Step 0: Move the settings fixture into `conftest.py`**

Add to `tests/conftest.py`, importing `Settings` alongside the existing `get_settings`:

```python
@pytest.fixture
def relay_settings(broker: str, topic: str) -> Settings:
    """Settings as the relay would read them, pointed at this test's broker and topic."""
    return Settings(kafka_bootstrap_servers=broker, kafka_topic=topic)
```

Then delete the local `settings` fixture from `tests/unit/test_relay_loop.py` and rename the five `settings: Settings` parameters to `relay_settings: Settings`, along with the `settings=settings` and `settings.kafka_topic` uses inside those five tests. Leave `test_a_cycle_retires_only_the_events_the_broker_confirmed` alone — it builds its own local `settings` pointed at a dead broker and never takes the fixture. `uv run pytest tests/unit/test_relay_loop.py` must still show 7 passed before moving on.

- [ ] **Step 1: Write the failing test**

Create `tests/invariants/test_i1_no_loss.py`:

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

First, prove the test is testing the reclaimer and not passing for free — comment out the `reclaim_expired` call and run:

Run: `uv run pytest tests/invariants/test_i1_no_loss.py -v`
Expected, case by case — the three differ, and each difference says something:

- `after_claim` FAILS on `assert len(read_events(...)) == 1` → `assert 0 == 1`. Nothing was ever published, which is the loss the invariant is named for. It first waits out `READ_TIMEOUT` (30 s) proving the topic is empty; that is the assertion working, not a hang.
- `after_publish_before_mark` FAILS on `assert statuses(...) == ["published"]` → `['publishing'] != ['published']`. The read passes here, because the first cycle did reach Kafka before dying — so this case shows the row stranded rather than the event lost.
- `after_mark` PASSES. It never needed recovery: the row was already retired when the seam fired.

Restore the call before Step 3. That check is the point of the task — a no-loss test that passes without the reclaimer is asserting nothing.

- [ ] **Step 3: Run the test to verify it passes**

Run: `uv run pytest tests/invariants/test_i1_no_loss.py -v`
Expected: 3 passed.

- [ ] **Step 4: Lint and type-check**

Run: `uv run ruff check . --fix && uv run pyright`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add tests/invariants/test_i1_no_loss.py tests/conftest.py tests/unit/test_relay_loop.py
git commit -m "test: prove I1 -- no loss at any of the three relay seams"
```

---

### Task 3: I3 — no double-claim, no double-reclaim

**Files:**
- Create: `tests/invariants/test_i3_no_double_claim.py`

The existing unit test named `test_two_relays_claiming_at_once_get_disjoint_rows` does not do what its name says: it calls the two claims one after the other, and `claim_batch` commits before returning, so the second relay passes over rows that are already `publishing` and no concurrent path is exercised at all. Task 1 renames it to match what it actually pins. This module is where two relays genuinely overlap, in threads released together through a barrier.

**Interfaces:**
- Consumes: `claim_batch`, `reclaim_expired`, `ClaimedEvent`, `write_pending_events`, `expire_leases`, `statuses`; the `relay_sessions` and `sync_engine` fixtures. No broker: nothing here publishes.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Create `tests/invariants/test_i3_no_double_claim.py`:

```python
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
```

- [ ] **Step 2: Run the tests, then prove they grip**

These do not start red, and the plan says so rather than dressing it up: I3 is a property of the two statements Task 1 already shipped, so a test-first ordering is not available here. That makes the mutation check below the only evidence the tests are worth keeping, rather than a nicety.

Run: `uv run pytest tests/invariants/test_i3_no_double_claim.py -v`
Expected: both PASS.

**Do not try to break these by dropping `skip_locked=True` — that mutation does not fail, and I measured it rather than assuming.** On Postgres 18 with 200 abandoned rows and two relays, removing `SKIP LOCKED` still yields 100 and 100, all 200 `pending`: the second relay blocks for the whole of the first's transaction (3.1 s in the probe), then re-reads the rows it waited for, finds them `pending`, and takes the other hundred. Correct, just serialised.

The mutation that *does* fail is the refactor this test exists to prevent — splitting the single statement into a read and then a write. Temporarily rewrite `reclaim_expired` as two statements in one transaction:

```python
    with session.begin():
        ids = list(session.execute(select(Outbox.id).where(...).limit(batch_size)).scalars())
        reclaimed = len(session.execute(update(Outbox).where(Outbox.id.in_(ids)).values(...)
                                        .returning(Outbox.id)).scalars().all())
```

Expected: `test_two_relays_reclaiming_at_once_never_recover_one_row_twice` FAILS on `assert statuses(sync_engine) == ["pending"] * TOTAL` — both relays select the same hundred ids, both report reclaiming a hundred, and the other hundred are left in `publishing`. Restore the single statement.

That failure depends on both relays reading before either writes, so it is the interleaving that makes it visible, not a guarantee. If it passes, raise `TOTAL` and re-run before concluding anything.

- [ ] **Step 3: Run the whole suite**

Run: `make test`
Expected: 68 passed (59 before this plan, plus 4 in Task 1, 3 in Task 2, 2 here).

- [ ] **Step 4: Lint and type-check**

Run: `uv run ruff check . --fix && uv run pyright`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add tests/invariants/test_i3_no_double_claim.py
git commit -m "test: prove I3 -- two relays never hold one outbox row at once"
```

---

## Verification

Facts this plan leans on, checked rather than assumed. An executor who finds one of them false should stop rather than work around it.

- **`SKIP LOCKED` with `LIMIT` returns a full batch.** `EXPLAIN` on Postgres 18 gives `Limit → LockRows → Index Scan using ix_outbox_reclaim`, so rows skipped by `LockRows` are never counted against the limit — and the partial index is used, confirming the `ORDER BY locked_until`. Verified live too: with one relay holding 100 rows locked, a second gets exactly 100. That is why the I3 counts are deterministic rather than lucky. Had `LockRows` sat above `Limit`, the second relay would get fewer and both counts would be arbitrary.
- **`SKIP LOCKED` buys liveness here, not correctness**, and the plan says so at the point of use. PostgreSQL's own words: it "can be used to avoid lock contention with multiple consumers accessing a queue-like table" ([SELECT](https://www.postgresql.org/docs/18/sql-select.html)). What prevents a double-reclaim is the sentence just above it — rows "will not be returned if they were updated after the snapshot and no longer satisfy the query conditions" — which is the `status='publishing'` predicate re-read after a lock wait. Both were measured on Postgres 18 before this plan was finalised, not recalled.
- **The claim and the reclaim cannot race each other**, only their own kind: the claim selects `status='pending'` and the reclaim `status='publishing'`, which are disjoint. Concurrency between the two needs no test, and the barrier tests pair like with like for that reason.
- **`attempts` is not incremented by the reclaim.** Spec §7.2 makes `attempts` the evidence during drills, so a reclaim-plus-claim counting two attempts for one delivery would corrupt exactly the number the drills read.
- **A reclaimed batch shares one `next_attempt_at`.** The claim's `ORDER BY next_attempt_at, id` therefore falls through to `id`, and `id` is UUIDv7, so write order survives the round trip and I5 stays intact. This is the reason the claim's tie-break exists.
- **Ageing the lease is the repo's established move, not a shortcut.** `test_a_row_not_yet_due_is_left_alone` already moves `next_attempt_at`; `expire_leases` moves `locked_until`. The clock-dependence itself is still asserted, by `test_a_live_lease_is_left_alone` — the case where the lease has *not* expired.
- **The fault must come off before the second cycle**, or every I1 case fails on the recovery rather than on the loss. `monkeypatch.context()` is the documented way to scope that: "there is generally no need to call `undo()`, since it is called automatically during tear-down" ([pytest reference](https://docs.pytest.org/en/stable/reference/reference.html#monkeypatch)), and `context()` is what the docs point to when a patch has to be lifted mid-test.

## What this plan does not prove

- **The reclaim is not scheduled.** Only tests call it. Until plan 4's daemon loop runs it on `RECLAIM_INTERVAL`, a stranded row in a live deployment stays stranded.
- **The ack-before-mark duplicate window is still open, permanently.** I1 passes at that seam by delivering the event twice. Spec §6.2 says this cannot be closed and §6.3 says why it does not matter; I4 in M3 is where one email despite two events gets asserted.
- **A rejected publish still has nowhere to go.** With this plan its row is reclaimed and retried forever, with no backoff and no classification. For a poison message that is an infinite loop — which is precisely what plan 3 exists to fix, and why it is next.

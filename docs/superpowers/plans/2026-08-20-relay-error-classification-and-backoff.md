# Relay Error Classification and Backoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every rejected publish a resolution in the cycle that saw it, so no rejected row is left waiting on its lease to expire.

**Architecture:** The relay currently marks what Kafka acked and ignores what it refused. A refused row keeps its lease, the lease expires, `reclaim_expired` returns it to `pending`, and it is tried again — immediately, forever, whether or not trying again could ever work. That is fine for a broker outage and wrong for a poison message, and it is slow for both, because every retry waits out a whole lease.

This plan splits a rejection by class and resolves it on the spot. `publisher.py` gains the classification (spec §10 already assigns it there); `outbox.py` gains the two transitions that act on it, alongside the three it already has; `loop.py` routes each verdict to one of them. Retriable rows go back to `pending` with a jittered backoff and their error recorded. Non-retriable rows are parked as `dead`, which is invisible to both the claim and the reclaim — that is what ends the loop.

The reclaimer keeps exactly one job after this: rows with **no verdict at all**, where the publish neither succeeded nor reported a failure. Those still wait out the lease, because there is nothing else to go on.

**Tech Stack:** Python 3.14 · `confluent-kafka` 2.15.0 (`KafkaError` codes) · SQLAlchemy 2.0 (sync Core, explicit `session.begin()`) · PostgreSQL 18 · `random.uniform` full jitter · pytest + testcontainers

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§7.1 timings and backoff, §7.2 error classification, §7.3 why not Kafka transactions, §10 file responsibilities)

## Global Constraints

- Python `==3.14.*`; every dependency pinned exactly. **This plan adds no dependency** — `random` is stdlib and `confluent_kafka.KafkaError` is already imported in `publisher.py`.
- Relay state transitions live in `relay/outbox.py`; error classification lives in `relay/publisher.py` (spec §10). Neither gets a new module.
- Classification is by error **code**. The rule is spec §7.2's: dead-letter only when the *message* is the problem; retry anything that is a property of the *environment*. That makes the poison set closed and enumerable, so it is a denylist and anything unrecognised retries. Never `KafkaError.retriable()` — see Task 1.
- Backoff is full jitter, `uniform(0, min(cap, base * 2**(attempts - 1)))`, base 1 s, cap 60 s (spec §7.1).
- A retriable failure is **never** dead-lettered, however many times it has failed (spec §7.2). No attempt ceiling exists anywhere in this plan.
- `settings` env prefix stays `OUTBOX_`; `ruff` line-length 100; `pyright` strict over `src`, `tests`, `migrations`.
- `relay` imports `shared` only, never `api` or `notifier` (§4.1). `outbox.py` must not import from `publisher.py` — the dependency runs the other way and `ClaimedEvent` is why.
- No migration. `dead` is already in `OutboxStatus` and `last_error` is already a column.
- Test helpers that are plain functions go in a sibling module under `tests/`; only fixtures go in `conftest.py` (§9.5).

## Out of Scope

- **Copying a dead event to `orders.events.dlq`** (plan 4). Marking `dead` is safe on its own and loses nothing: the row keeps its payload, gains `last_error`, and is queryable and replayable where it sits. The DLQ *topic* copy is a publish that can itself fail, which raises its own ordering question — copy-then-mark leaves a duplicate window, mark-then-copy can drop the copy — and that deserves a plan rather than a paragraph.
- **Scheduling anything** (plan 5). Nothing calls `run_once` or `reclaim_expired` in production yet; the daemon loop, `RECLAIM_INTERVAL`, and signal handling still belong to the compose plan. Stated consequence: after this plan the classification is correct but only tests exercise it.
- **I6 (backlog and drain).** It needs the broker stopped and restarted, and the `broker` fixture is session-scoped — stopping it would break every other test in the run. I6 belongs with the compose stack, where a broker can be killed without taking the suite with it.
- **An attempt ceiling, a poison *detector*, or alerting on `outbox_oldest_pending_seconds`.** The first contradicts §7.2 outright; the last two are observability work with no home yet.

## Plan Queue

M2 is split across small plans. Plans 1 and 2 have shipped; **this is plan 3**.

1. ~~Claim, publish, mark — one relay cycle~~ — `2026-08-19-relay-publish-cycle.md` (shipped, PR #4)
2. ~~Reclaim expired leases — proves I1 and I3~~ — `2026-08-19-relay-reclaim-expired-leases.md` (shipped, PR #5)
3. **[this plan]** Error classification and backoff — `2026-08-20-relay-error-classification-and-backoff.md`
4. Copy a dead event to `orders.events.dlq` — write after plan 3 ships
5. Kafka and the relay under compose, with chaos targets — write after plan 4 ships

## Spec Amendments (already applied)

The spec was updated alongside this plan, so it cannot drift during execution. No task needs to touch it:

- **§7.1** — the backoff row now says **full** jitter and gives the formula, and a new paragraph names the two settings, cites AWS's measurements for preferring full jitter over equal and decorrelated, and states the drawback (the delay may collapse toward zero) along with why it does not bite here.
- **§7.2** — two additions. First, that the classification is a **denylist with retry as the default**, and why the asymmetry runs that way. Second, that `KafkaError.retriable()` is the transactional producer's flag and must not be used, with the measurement that shows why.

---

### Task 1: Classify a rejection, and give a locally rejected event a verdict

**Files:**
- Modify: `src/outboxexpress/relay/publisher.py` (add the code table and `is_retriable`; change the `produce` loop)
- Test: `tests/unit/test_relay_publisher.py` (six new tests)

Both halves are the publisher's error story, which is why they share a task: one says what a failure *means*, the other makes sure a failure that arrives by a different route still becomes a verdict rather than an exception.

The list is transcribed, not invented. Three sources, and where they disagree spec §7.2 records which one wins and why: [Kafka's protocol error-code table](https://kafka.apache.org/43/design/protocol) has a `Retriable` column for every broker code; [librdkafka's INTRODUCTION.md](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md) gives its own permanent-vs-temporary split plus the local `_`-prefixed codes that no broker table covers; and [Kafka Connect's DLQ](https://developer.confluent.io/courses/kafka-connect/error-handling-and-dead-letter-queues/) draws the same line this does — bad *records* are dead-lettered, bad *infrastructure* is not.

The second half fixes something plan 1 flagged and left. `producer.produce()` rejects some messages locally and synchronously — an oversized payload is the documented case — raising `KafkaException` instead of reporting through the delivery callback. Today that propagates out of `publish_batch`, out of `run_once`, and the row is left in `publishing` for the reclaimer, which returns it to `pending`, which produces it again, which raises again. Measured on 2.15.0: a 2 MB value raises `KafkaException` carrying `MSG_SIZE_TOO_LARGE` (code 10) with no broker involved, because librdkafka's default `message.max.bytes` is 1 000 000 and the check is local.

**Interfaces:**
- Consumes: `ClaimedEvent` (`relay/outbox.py`); the `broker` and `topic` fixtures; `an_event` and `header` (already in `tests/unit/test_relay_publisher.py`).
- Produces: `is_retriable(error: KafkaError) -> bool` and `NON_RETRIABLE_CODES: frozenset[int]` in `relay/publisher.py`. `publish_batch`'s signature and return type are unchanged — only its behaviour on a local rejection changes.

- [ ] **Step 1: Write the failing classification tests**

Append to `tests/unit/test_relay_publisher.py`, and add `KafkaError` and `is_retriable` to its imports:

```python
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_publisher.py -v`
Expected: FAIL — `ImportError: cannot import name 'is_retriable'`

- [ ] **Step 3: Write the classification**

In `src/outboxexpress/relay/publisher.py`, add `KafkaException` to the `confluent_kafka` import and insert after `log = get_logger(__name__)`:

```python
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
```

- [ ] **Step 4: Run the classification tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_publisher.py -v`
Expected: PASS, 5 new tests among them

- [ ] **Step 5: Write the failing local-rejection test**

Append to `tests/unit/test_relay_publisher.py`:

```python
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
```

- [ ] **Step 6: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_relay_publisher.py -k locally_rejected -v`
Expected: FAIL with `KafkaException: KafkaError{code=MSG_SIZE_TOO_LARGE...}` — the exception escapes `publish_batch`

- [ ] **Step 7: Capture the rejection as a verdict**

In `publish_batch`, replace the produce loop and the comment above it. The old comment justified the outer `finally` in terms of `produce()` raising mid-loop, which no longer happens, so it is rewritten rather than kept:

```python
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
                # produce() applies some checks locally and synchronously -- the
                # message size is the documented one -- and raises instead of
                # reporting. Recording the verdict here is what lets the caller
                # resolve this one event and keep producing the rest of the batch.
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
```

- [ ] **Step 8: Run the publisher suite to verify it passes**

Run: `uv run pytest tests/unit/test_relay_publisher.py -v`
Expected: PASS, all 11

- [ ] **Step 9: Commit**

```bash
git add src/outboxexpress/relay/publisher.py tests/unit/test_relay_publisher.py
git commit -m "feat: classify a rejected publish, and give a local rejection a verdict"
```

---

### Task 2: Return a retriable failure to the queue with a backoff

**Files:**
- Modify: `src/outboxexpress/shared/config.py:32` (two settings after `relay_poll_interval_seconds`)
- Modify: `src/outboxexpress/relay/outbox.py` (add `FailedEvent`, `retry_ceiling_seconds`, `retry_delay`, `reschedule_failed`)
- Test: `tests/unit/test_relay_outbox.py` (seven new tests), `tests/unit/test_config.py:19-24` (two assertions)

The ceiling and the jitter are separate functions on purpose. The ceiling is the policy — doubling, capped, clamped — and it is deterministic, so it can be asserted exactly. The jitter is one line of randomness on top. Folded together they would only be testable through statistics, and a test that samples a distribution is a test that fails on a Friday.

`reschedule_failed` reads each row's `attempts` itself rather than having it carried down from the claim. `ClaimedEvent` is documented as "what the publisher needs and nothing else", the publisher has no use for `attempts`, and one extra `SELECT` inside a transaction that is already doing per-row work costs nothing.

**Interfaces:**
- Consumes: `Outbox`, `OutboxStatus` (`shared/models.py`); `claim_batch` and `mark_published` (same module); `write_pending_events`, `expire_leases`, `statuses`, `row_state`; the `relay_sessions` and `sync_engine` fixtures.
- Produces: `FailedEvent(event_id: UUID, error: str)`, `retry_ceiling_seconds(attempts: int, *, base_seconds: float, cap_seconds: float) -> float`, `retry_delay(attempts: int, *, base_seconds: float, cap_seconds: float) -> timedelta`, and `reschedule_failed(session: Session, failures: Sequence[FailedEvent], *, instance_id: str, base_seconds: float, cap_seconds: float) -> int`. Settings `relay_backoff_base_seconds: float = 1.0` and `relay_backoff_cap_seconds: float = 60.0`.

- [ ] **Step 1: Write the failing backoff-policy tests**

Append to `tests/unit/test_relay_outbox.py`, adding `FailedEvent`, `retry_ceiling_seconds`, `retry_delay` to the `outboxexpress.relay.outbox` import and `from datetime import timedelta` at the top:

```python
BASE = 1.0
CAP = 60.0


def test_the_backoff_ceiling_doubles_with_each_attempt() -> None:
    # attempts is already 1 when the first publish fails, because the claim counted it.
    # So attempt 1 waits within 1s, per spec 7.1's "1 s, x2".
    ceilings = [
        retry_ceiling_seconds(attempts, base_seconds=BASE, cap_seconds=CAP)
        for attempts in range(1, 6)
    ]
    assert ceilings == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_the_backoff_ceiling_stops_at_the_cap() -> None:
    assert retry_ceiling_seconds(20, base_seconds=BASE, cap_seconds=CAP) == CAP


def test_a_long_outage_does_not_overflow_the_ceiling() -> None:
    # attempts has no ceiling by design: a retriable failure retries forever. At one
    # attempt per delivery timeout, 2000 of them is under a day -- and 1.0 * 2**2000
    # raises OverflowError, so the exponent clamp is load-bearing, not defensive.
    assert retry_ceiling_seconds(2_000, base_seconds=BASE, cap_seconds=CAP) == CAP


def test_the_delay_is_jittered_inside_the_ceiling() -> None:
    delays = {retry_delay(4, base_seconds=BASE, cap_seconds=CAP) for _ in range(50)}

    # Jitter is the whole point: without it, N relays that failed on one broker
    # outage would come back in lockstep and fail together again.
    assert len(delays) > 1
    assert all(timedelta() <= delay <= timedelta(seconds=8) for delay in delays)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: FAIL — `ImportError: cannot import name 'retry_ceiling_seconds'`

- [ ] **Step 3: Write the backoff policy**

In `src/outboxexpress/relay/outbox.py`, add `import random` at the top and append after `reclaim_expired`:

```python
# 2**32 seconds is 136 years, so clamping here cannot shorten a delay the cap would
# have allowed. It exists because attempts is unbounded -- a retriable failure retries
# forever -- and 1.0 * 2**2000 raises OverflowError rather than returning inf.
_BACKOFF_EXPONENT_LIMIT = 32


def retry_ceiling_seconds(attempts: int, *, base_seconds: float, cap_seconds: float) -> float:
    """The longest a row may wait before its next attempt (spec 7.1: 1 s, x2, cap 60 s).

    Deterministic, and separate from the jitter that draws against it, so the policy
    can be asserted exactly instead of sampled.
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
```

- [ ] **Step 4: Run the policy tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_outbox.py -k "ceiling or jittered" -v`
Expected: PASS, 4 tests

- [ ] **Step 5: Write the failing transition tests**

Append to `tests/unit/test_relay_outbox.py`:

```python
def test_a_retriable_failure_goes_back_to_pending_with_its_error_recorded(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        moved = reschedule_failed(
            session,
            [FailedEvent(event_id=event_id, error="Local: Message timed out")],
            instance_id="relay-1",
            base_seconds=BASE,
            cap_seconds=CAP,
        )

    assert moved == 1
    state = row_state(sync_engine, event_id)
    assert state["status"] == "pending"
    assert state["last_error"] == "Local: Message timed out"
    assert state["locked_by"] is None
    assert state["locked_until"] is None
    # Counted once, by the claim. The next claim counts the next attempt.
    assert state["attempts"] == 1


def test_a_rescheduled_row_is_due_no_later_than_its_ceiling(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # The only bound full jitter gives is the upper one -- see retry_delay. Asserting
    # a minimum wait would be asserting something the policy does not promise.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
    before = utcnow()

    with relay_sessions() as session:
        reschedule_failed(
            session,
            [FailedEvent(event_id=event_id, error="boom")],
            instance_id="relay-1",
            base_seconds=BASE,
            cap_seconds=CAP,
        )

    # One bound, with slack. Full jitter promises no minimum wait, so there is no
    # lower bound to assert; and `before` comes off the host clock while
    # next_attempt_at comes off the container's, so the upper bound carries a second
    # of tolerance rather than pretending the two clocks are one.
    due = row_state(sync_engine, event_id)["next_attempt_at"]
    assert due <= before + timedelta(seconds=BASE + 1)


def test_a_relay_cannot_reschedule_a_row_another_relay_holds(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # Same guard, and the same reason, as mark_published: a relay whose lease expired
    # mid-publish must not push back a row another relay has since taken and may
    # already have published.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        assert (
            reschedule_failed(
                session,
                [FailedEvent(event_id=event_id, error="boom")],
                instance_id="relay-2",
                base_seconds=BASE,
                cap_seconds=CAP,
            )
            == 0
        )

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_rescheduling_nothing_touches_nothing(relay_sessions: sessionmaker[Session]) -> None:
    with relay_sessions() as session:
        assert (
            reschedule_failed(
                session, [], instance_id="relay-1", base_seconds=BASE, cap_seconds=CAP
            )
            == 0
        )
```

- [ ] **Step 6: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: FAIL — `ImportError: cannot import name 'reschedule_failed'`

- [ ] **Step 7: Write the transition**

In `src/outboxexpress/relay/outbox.py`, add `FailedEvent` below `ClaimedEvent`:

```python
@dataclass(frozen=True)
class FailedEvent:
    """One rejected event, and what the broker said about it.

    ``error`` is already rendered to text: the classification happened in
    ``publisher``, and this module has no business re-reading a ``KafkaError``.
    """

    event_id: UUID
    error: str
```

and append after `retry_delay`:

```python
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
```

- [ ] **Step 8: Add the two settings**

In `src/outboxexpress/shared/config.py`, after `relay_poll_interval_seconds`:

```python
    # Spec 7.1. Full jitter draws uniformly below a ceiling that doubles per attempt
    # and stops at the cap; tests shrink both so a backoff does not cost real seconds.
    relay_backoff_base_seconds: float = 1.0
    relay_backoff_cap_seconds: float = 60.0
```

and extend `test_relay_settings_carry_the_values_the_spec_fixes` in `tests/unit/test_config.py`:

```python
    assert settings.relay_backoff_base_seconds == 1.0
    assert settings.relay_backoff_cap_seconds == 60.0
```

- [ ] **Step 9: Run the outbox and config suites to verify they pass**

Run: `uv run pytest tests/unit/test_relay_outbox.py tests/unit/test_config.py -v`
Expected: PASS, 20 and 4

- [ ] **Step 10: Commit**

```bash
git add src/outboxexpress/relay/outbox.py src/outboxexpress/shared/config.py \
        tests/unit/test_relay_outbox.py tests/unit/test_config.py
git commit -m "feat: return a retriably-failed row to pending with jittered backoff"
```

---

### Task 3: Park a poison message

**Files:**
- Modify: `src/outboxexpress/relay/outbox.py` (add `mark_dead` below `reschedule_failed`)
- Test: `tests/unit/test_relay_outbox.py` (four new tests)

The test that matters here is the third one. `dead` is worth having only because it is invisible to *both* other scans — `claim_batch` takes `pending`, `reclaim_expired` takes `publishing` — and that is what actually ends the retry loop. Nothing in this task's implementation asserts it; the test does, and it would catch a later widening of either predicate.

**Interfaces:**
- Consumes: `FailedEvent`, `claim_batch`, `reclaim_expired` (same module); `expire_leases`, `row_state`; the `relay_sessions` and `sync_engine` fixtures.
- Produces: `mark_dead(session: Session, failures: Sequence[FailedEvent], *, instance_id: str) -> int`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_relay_outbox.py`, adding `mark_dead` to the import:

```python
def test_a_poison_message_is_parked_with_its_error(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        parked = mark_dead(
            session,
            [FailedEvent(event_id=event_id, error="Broker: Message size too large")],
            instance_id="relay-1",
        )

    assert parked == 1
    state = row_state(sync_engine, event_id)
    assert state["status"] == "dead"
    assert state["last_error"] == "Broker: Message size too large"
    assert state["locked_by"] is None
    assert state["locked_until"] is None
    # Not published, so published_at stays empty: the event never reached the topic.
    assert state["published_at"] is None


def test_a_dead_row_is_invisible_to_the_claim_and_the_reclaim(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # This is what ends the loop. Before this plan a rejected row went publishing ->
    # reclaimed -> pending -> claimed -> rejected, forever. `dead` is outside both
    # scans' predicates, so neither of them can pick it up again.
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
        mark_dead(session, [FailedEvent(event_id=event_id, error="poison")], instance_id="relay-1")
    expire_leases(sync_engine)

    with relay_sessions() as session:
        assert reclaim_expired(session, instance_id="relay-2", batch_size=10) == 0
        assert claim_batch(session, instance_id="relay-2", batch_size=10, lease_seconds=LEASE) == []

    assert row_state(sync_engine, event_id)["status"] == "dead"


def test_a_relay_cannot_kill_a_row_another_relay_holds(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    with relay_sessions() as session:
        assert (
            mark_dead(
                session, [FailedEvent(event_id=event_id, error="poison")], instance_id="relay-2"
            )
            == 0
        )

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_marking_nothing_dead_touches_nothing(relay_sessions: sessionmaker[Session]) -> None:
    with relay_sessions() as session:
        assert mark_dead(session, [], instance_id="relay-1") == 0
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: FAIL — `ImportError: cannot import name 'mark_dead'`

- [ ] **Step 3: Write the transition**

Append to `src/outboxexpress/relay/outbox.py`:

```python
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
```

- [ ] **Step 4: Run the outbox suite to verify it passes**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: PASS, 24

- [ ] **Step 5: Commit**

```bash
git add src/outboxexpress/relay/outbox.py tests/unit/test_relay_outbox.py
git commit -m "feat: park a non-retriable outbox row as dead"
```

---

### Task 4: Route every verdict in the cycle

**Files:**
- Modify: `src/outboxexpress/relay/loop.py:19-20,60-71` (imports, the routing, the three transitions, the log line)
- Test: `tests/unit/test_relay_loop.py` (two new tests, and one existing test's expectations change)

`test_a_cycle_retires_only_the_events_the_broker_confirmed` currently ends with `statuses(sync_engine) == ["published", "publishing"]` and a comment saying the reclaimer in plan 2 is what brings the rejected row back. Its fake rejects with `MSG_SIZE_TOO_LARGE`, which this plan classifies as poison, so the row now becomes `dead` in the same cycle. The test's *purpose* is unchanged — `published` still implies Kafka acked it — so it keeps its name and gets a new expectation and a new comment. Note the ordering holds: `statuses` orders by `next_attempt_at, id`, and neither `mark_published` nor `mark_dead` moves that column.

`hooks.after_mark()` stays immediately after `mark_published`, where its docstring's meaning ("the rows are retired") still holds. A crash there leaves the failures unresolved in `publishing`, which the reclaimer already handles — the same recovery every other seam relies on.

**Interfaces:**
- Consumes: `is_retriable` (`relay/publisher.py`); `FailedEvent`, `reschedule_failed`, `mark_dead` (`relay/outbox.py`); `statuses` (`tests/outbox_state.py`); the `relay_sessions`, `relay_settings`, `sync_engine` fixtures.
- Produces: nothing new. `run_once`'s signature and its `int` return (rows retired) are unchanged, so plan 5's daemon is unaffected.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_relay_loop.py`, restore `text` to the `sqlalchemy` import (`from sqlalchemy import Engine, text`) — plan 2 removed it when `statuses` moved out. `KafkaError` and `statuses` are already imported. Then append:

```python
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
```

and change the tail of `test_a_cycle_retires_only_the_events_the_broker_confirmed`:

```python
    # The rejected row is resolved in this same cycle rather than left for the
    # reclaimer: MSG_SIZE_TOO_LARGE is poison, so it is parked. What must never
    # happen is it being retired alongside its sibling.
    assert marked == 1
    assert statuses(sync_engine) == ["published", "dead"]
```

- [ ] **Step 2: Run them, and note which one does not fail**

Run: `uv run pytest tests/unit/test_relay_loop.py -v`
Expected: 2 failed, 8 passed.

- `test_a_retriable_rejection_returns_the_row_to_pending_with_its_error` FAILS: `assert ['publishing'] == ['pending']`.
- `test_a_cycle_retires_only_the_events_the_broker_confirmed` FAILS: `assert ['published', 'publishing'] == ['published', 'dead']`.
- `test_an_event_with_no_verdict_keeps_its_lease_for_the_reclaimer` **passes already**, and that is the honest position rather than a TDD failure to paper over: an unreported event is the one case this plan deliberately does not change. It goes in as a regression guard, so that a later plan widening the routing cannot silently start resolving rows it has no verdict for.

- [ ] **Step 3: Route the verdicts**

In `src/outboxexpress/relay/loop.py`, extend the imports:

```python
from .outbox import FailedEvent, claim_batch, mark_dead, mark_published, reschedule_failed
from .publisher import flush_timeout_seconds, is_retriable, publish_batch
```

then replace everything from `delivered = [...]` to the end of `run_once`. The annotation uses `uuid.UUID` because the module already imports `uuid` for `new_instance_id`:

```python
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
```

- [ ] **Step 4: Run the loop suite to verify it passes**

Run: `uv run pytest tests/unit/test_relay_loop.py -v`
Expected: PASS, 9

- [ ] **Step 5: Run everything, then lint and type-check**

Run: `make test && make lint`
Expected: PASS, 88 tests (68 before this plan: +6 publisher, +12 outbox, +2 loop; `test_config` and one loop test are modified rather than added). `ruff check .` clean, `pyright` 0 errors.

- [ ] **Step 6: Prove the tests grip**

Two mutations, each run and then reverted. Back the files up first (`cp` to a scratch path) and restore them byte-identically after — `git diff` must be empty before committing.

1. In `publisher.py`, change `is_retriable` to `return error.retriable()`.
   Expected: `test_classification_does_not_use_librdkafkas_transactional_flag` FAILS, and so does `test_a_retriable_rejection_returns_the_row_to_pending_with_its_error` — the outage row is parked as `dead`. This is the mutation that matters: it is the plausible "simplification" a future reader would reach for.
2. In `outbox.py`, change `_BACKOFF_EXPONENT_LIMIT` to `2_000`.
   Expected: `test_a_long_outage_does_not_overflow_the_ceiling` FAILS with `OverflowError`.

- [ ] **Step 7: Commit**

```bash
git add src/outboxexpress/relay/loop.py tests/unit/test_relay_loop.py
git commit -m "feat: resolve every publish verdict in the cycle that produced it"
```

---

## Verification

- `make test` — 88 passed
- `make lint` — `ruff check .` clean; `pyright` strict, 0 errors
- Both mutations in Task 4 Step 6 fail the test that claims to cover them, and both files are restored byte-identically (`git diff` empty)
- No new dependency, no migration

## What this plan does not prove

- **That a dead event reaches `orders.events.dlq`.** Nothing publishes to the DLQ topic yet — plan 4. A dead row is parked and queryable, not exported.
- **That a real backlog drains (I6).** It needs the broker stopped mid-run, which the session-scoped `broker` fixture cannot survive. It lands with the compose stack.
- **That any of this runs unattended.** Still no `__main__.py` and no daemon loop; only tests call `run_once` and `reclaim_expired`. Plan 5.
- **That the retriable code list is complete.** It cannot be — that is the point of the denylist. What the tests pin is the direction the default falls, and that the two codes most likely to be misclassified are on the right side of it.

# Relay Publish Cycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move a batch of pending outbox rows to Kafka in one relay cycle: claim under a lease, publish, mark published.

**Architecture:** Two short transactions with the network call between them, exactly as spec §6.2 draws it. `outbox.py` owns every state transition the relay makes to the table — the claim and the mark today, the reclaim and the dead-letter in later plans — because they are one state machine over one table with one reason to change. `publisher.py` owns the producer and knows nothing about Postgres; `loop.py` is the narrative that joins them and is the only module that calls the failure-injection hooks.

The relay is synchronous. Spec §3.3 and §3.5 settle this: `confluent-kafka` is a sync client and `relay` is a plain worker process, so `psycopg3`'s sync side and a plain `Session` cover it with no asyncio bridge and no `pytest-asyncio`.

**Tech Stack:** Python 3.14 · `confluent-kafka` 2.15.0 (librdkafka) · SQLAlchemy 2.0 (sync Core statements, explicit `session.begin()`) · PostgreSQL 18 `FOR UPDATE SKIP LOCKED` · pytest + testcontainers (Postgres 18, `apache/kafka-native:4.3.1`)

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§6.2 relay path, §7.1 timings, §8.1 topics, §8.2 producer, §9.1 hooks, §9.2 I5)

## Global Constraints

- Python `==3.14.*`; every dependency pinned exactly, as in `pyproject.toml` today.
- `confluent-kafka==2.15.0` (spec §3.6). No other new runtime dependency. It ships `py.typed` and `cimpl.pyi` at this version, so `pyright` strict reads it directly — **do not add `types-confluent-kafka` or `confluent-kafka-stubs`**; much of the advice online predates the library's own typing. If pyright disagrees with a `cimpl` signature, annotate at the call site.
- Producer config is fixed by §8.2: `acks=all`, `enable.idempotence=true`, `compression.type=lz4`, `linger.ms=5`, `delivery.timeout.ms=30000`.
- Relay timings are fixed by §7.1: `BATCH_SIZE=100`, `POLL_INTERVAL=500 ms`, `LEASE_SECONDS=60`.
- **`delivery.timeout.ms` must be strictly less than `LEASE_SECONDS`** (§7.1). Violating it manufactures duplicates on every slow publish.
- Topic `orders.events`: 3 partitions, message key is `aggregate_id` (§8.1). Tests use RF=1 against a single broker (§9.3); compose supplies RF=3 in plan 4.
- `settings` env prefix stays `OUTBOX_`; `ruff` line-length 100; `pyright` strict over `src`, `tests`, `migrations`.
- `relay` imports `shared` only. It must not import `api` or `notifier` (§4.1).

## Out of Scope

- **The reclaimer** (plan 2). Consequence, stated rather than hidden: in this plan a publish that fails, or a crash between claim and mark, leaves the row in `publishing` with nothing to recover it. That is why I1 belongs to plan 2, not here.
- **Error classification, backoff, and the DLQ** (plan 3). A failed delivery here is simply not marked; §7.2's retriable/non-retriable split lands with the reclaimer that gives it somewhere to go. Two specifics for that plan: a `produce()` that raises mid-batch (an oversized payload, an invalid argument) currently aborts the cycle after draining, and it is a non-retriable error that belongs in the DLQ rather than on a retry loop; and `last_error` is the column it should write.
- **`relay/__main__.py`, the daemon loop, signal handling, and the compose service** (plan 4). This plan delivers one cycle, proved by `make test`, the same way the write path shipped before any HTTP surface existed. That plan owns the producer's lifetime: it must `flush()` on SIGTERM, or a shutdown mid-cycle abandons the producer queue. No events are lost when it does not -- their rows still say `publishing` -- but the drain then waits on a lease expiry instead of happening at once.
- **`kafka-ui`, the three-broker cluster, chaos targets** (plan 4).

## Plan Queue

M2 is split across small plans. **Only plan 1 is written.**

1. **[this plan]** Claim, publish, mark — one relay cycle — `2026-08-19-relay-publish-cycle.md`
2. Reclaim expired leases — proves I1 and I3 — write after plan 1 ships
3. Error classification, backoff, and DLQ routing — write after plan 2 ships
4. Kafka and the relay under compose, with chaos targets — write after plan 3 ships

## Spec Amendments (already applied)

The spec was updated alongside this plan, so it does not drift during execution. No task needs to touch it:

- **§8.2** now records that librdkafka accepts the producer config verbatim, and that `acks=all` is *required* to be consistent with `enable.idempotence` rather than merely allowed — librdkafka fails producer construction on a contradiction. It cites librdkafka's ordering guarantee, which is what I5 rests on.
- **§9.5** is new: where shared test code lives, and why fixtures are injected from `conftest.py` while plain helpers go in sibling modules.
- **§10** renames `relay/claim.py` to `relay/outbox.py` — the mark had no home under the old name, and by plan 3 the module holds four transitions. Procrastinate keeps the same four together in one module (`fetch_job`, `finish_job`, `retry_job`, `get_stalled_jobs`), for the same reason. The `tests/` tree gains the three helper modules.
- **§3.6 and §9.3** record that tests run `apache/kafka-native:4.3.1` while compose keeps `apache/kafka:4.3.1`.

---

### Task 1: A Kafka broker for the suite

**Files:**
- Modify: `pyproject.toml` (add `confluent-kafka==2.15.0`)
- Create: `tests/kafka_container.py`
- Modify: `tests/conftest.py` (add the `broker` and `topic` fixtures)
- Test: `tests/unit/test_broker_fixture.py`

The container gets its own file rather than joining `conftest.py`. Every test author reads `conftest.py`; a ported vendor recipe there is sixty lines of image-layout detail between them and the fixtures they came for. It also changes for a different reason — when the Apache image changes, or when testcontainers-python finally ships its own container and this file can be deleted.

**Interfaces:**
- Consumes: nothing.
- Produces: `ApacheKafkaContainer(image: str = "apache/kafka-native:4.3.1")` with `.bootstrap_server() -> str`; `broker` fixture (session scope) → `str` bootstrap server; `topic` fixture (function scope) → `str`, the name of a freshly created 3-partition topic unique to that test.

- [ ] **Step 1: Add the client dependency**

```bash
uv add 'confluent-kafka==2.15.0'
```

Confirm `pyproject.toml` gained `"confluent-kafka==2.15.0",` under `[project].dependencies` and that `uv.lock` changed. No testcontainers extra is needed — the Kafka module already ships in the installed 4.15.0.

- [ ] **Step 2: Write the failing test**

Create `tests/unit/test_broker_fixture.py`:

```python
"""The suite's broker. apache/kafka-native, single node, KRaft -- see spec 9.3."""

from confluent_kafka.admin import AdminClient


def test_the_topic_exists_with_three_partitions(broker: str, topic: str) -> None:
    metadata = AdminClient({"bootstrap.servers": broker}).list_topics(timeout=10)
    assert topic in metadata.topics
    assert len(metadata.topics[topic].partitions) == 3
```

- [ ] **Step 3: Run it to make sure it fails**

Run: `uv run pytest tests/unit/test_broker_fixture.py -v`
Expected: FAIL — `fixture 'broker' not found`.

- [ ] **Step 4: Port the upstream container recipe**

Create `tests/kafka_container.py`:

```python
"""A single-node Apache Kafka broker for the suite.

testcontainers-python ships only a Confluent-image container, and upstream
Testcontainers has since deprecated that shape in favour of one container for
``apache/kafka`` and ``apache/kafka-native``. This is their current recipe, ported.
Delete this file the day testcontainers-python ships its own.
"""

import re
import tarfile
import time
from io import BytesIO
from typing import Self

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy


class ApacheKafkaContainer(DockerContainer):
    """A single-node KRaft broker, ready once ``start()`` returns."""

    START_SCRIPT = "/tmp/testcontainers_start.sh"
    CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw"
    CLIENT_PORT = 9092
    BROKER_PORT = 9093
    CONTROLLER_PORT = 9094

    def __init__(self, image: str = "apache/kafka-native:4.3.1") -> None:
        super().__init__(image)
        self.with_exposed_ports(self.CLIENT_PORT)
        # The image waits for a script that cannot be written until the host port is
        # known -- see start(). Until then it must not launch.
        self.with_command(
            f'sh -c "while [ ! -f {self.START_SCRIPT} ]; do sleep 0.1; done; '
            f'sh {self.START_SCRIPT}"'
        )
        listeners = (
            f"PLAINTEXT://0.0.0.0:{self.CLIENT_PORT},"
            f"BROKER://0.0.0.0:{self.BROKER_PORT},"
            f"CONTROLLER://0.0.0.0:{self.CONTROLLER_PORT}"
        )
        for name, value in {
            "CLUSTER_ID": self.CLUSTER_ID,
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": listeners,
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
            ),
            "KAFKA_INTER_BROKER_LISTENER_NAME": "BROKER",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@localhost:{self.CONTROLLER_PORT}",
            # One node, so every internal topic is RF=1. Spec 9.3 states this gap.
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        }.items():
            self.with_env(name, value)

    def bootstrap_server(self) -> str:
        host = self.get_container_host_ip()
        return f"{host}:{self.get_exposed_port(self.CLIENT_PORT)}"

    def start(self) -> Self:
        super().start()
        # A client reaching the broker on a mapped port must be advertised that port,
        # and Docker only assigns it once the container is up -- hence the wait loop in
        # the command above and this script written into the running container.
        script = (
            "#!/bin/bash\n"
            f"export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{self.bootstrap_server()},"
            f"BROKER://$(hostname -i):{self.BROKER_PORT}\n"
            "/etc/kafka/docker/run\n"
        ).encode()
        self._put_file(script, self.START_SCRIPT)
        # "Kafka Server started" is the last line the broker logs, after "Awaiting
        # socket connections". Earlier markers -- the RECOVERY to RUNNING transition, for
        # one -- are printed before the listener accepts connections, so a client handed
        # bootstrap_server() at that point can be refused.
        strategy = LogMessageWaitStrategy(re.compile(r".*Kafka Server started.*"))
        strategy.with_startup_timeout(90)
        strategy.wait_until_ready(self)
        return self

    def _put_file(self, content: bytes, path: str) -> None:
        with BytesIO() as archive, tarfile.TarFile(fileobj=archive, mode="w") as tar:
            entry = tarfile.TarInfo(name=path)
            entry.size = len(content)
            entry.mtime = int(time.time())
            tar.addfile(entry, BytesIO(content))
            archive.seek(0)
            # docker-py ships no type information, so put_archive's signature is only
            # partially known to pyright. The boundary is one call wide.
            self.get_wrapped_container().put_archive(  # pyright: ignore[reportUnknownMemberType]
                "/", archive
            )
```

- [ ] **Step 5: Add the fixtures**

In `tests/conftest.py`, add to the imports:

```python
from concurrent.futures import Future
from typing import Protocol, cast  # cast is new; Protocol is already there
from uuid import uuid4

# NewTopic is the documented import path, but the admin package never re-exports it,
# so pyright strict reads it as private. The library's packaging is what is imprecise.
from confluent_kafka.admin import AdminClient, NewTopic  # pyright: ignore[reportPrivateImportUsage]
from kafka_container import ApacheKafkaContainer
```

`kafka_container` sorts into the *same* import group as `outboxexpress`, not a group of
its own: `[tool.ruff] src = ["src", "tests"]` makes `tests/` first-party too. Run
`ruff check . --fix` and let it place the line.

Beside the `VALID_ORDER` constant:

```python
TOPIC = "orders.events"
PARTITIONS = 3

# What create_topics really returns. The library annotates it as a bare ``Future``,
# which pyright strict reads as partially unknown, so the type is restated here once.
TopicFutures = dict[str, Future[None]]
```

And after the `clean_tables` fixture:

```python
@pytest.fixture(scope="session")
def broker() -> Iterator[str]:
    # Unlike `database`, this sets no environment variable: nothing reads the broker
    # address from settings in the suite -- every test builds the Settings it wants.
    with ApacheKafkaContainer() as container:
        yield container.bootstrap_server()


@pytest.fixture
def topic(broker: str) -> str:
    """A uniquely named topic per test, so every consumer starts at offset zero.

    Reusing one name and deleting between tests races Kafka's asynchronous deletion --
    the recreate fails with "marked for deletion". A new name cannot race anything.
    Partition count matches spec 8.1; replication is 1 because there is one broker.
    """
    name = f"{TOPIC}-{uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": broker})
    created = cast(
        TopicFutures,
        admin.create_topics(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            [NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)]
        ),
    )
    # Creation is asynchronous, and result() is what reports a rejected config: without
    # waiting here, the topic may not exist when the test produces to it.
    for future in created.values():
        future.result()
    return name
```

The `cast` cannot be replaced by an annotation on `created`: pyright narrows a declared
type to the assigned one, so the library's `Unknown` survives. Nor can it use an
intermediate variable -- that variable is itself partially unknown.

`from kafka_container import ...` without a package prefix matches how the existing tests already import `conftest` directly — pytest puts the `tests/` directory on `sys.path`.

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_broker_fixture.py -v`
Expected: PASS. First run pulls the image, so allow a minute.

If the wait strategy times out, read the container log for the real startup error before touching the env vars — a KRaft misconfiguration reports itself clearly there.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check . && uv run pyright
git add pyproject.toml uv.lock tests/kafka_container.py tests/conftest.py tests/unit/test_broker_fixture.py
git commit -m "test: run a single-node apache/kafka broker for the suite"
```

---

### Task 2: Relay settings and a sync session factory

**Files:**
- Modify: `src/outboxexpress/shared/config.py`
- Modify: `src/outboxexpress/shared/db.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: the existing `Settings` class and `get_settings()` in `shared/config.py`, and `create_sessionmaker` in `shared/db.py` as the pattern to mirror.
- Produces: `Settings.kafka_bootstrap_servers: str`, `.kafka_topic: str`, `.kafka_delivery_timeout_ms: int`, `.relay_batch_size: int`, `.relay_lease_seconds: int`, `.relay_poll_interval_seconds: float`; `create_sync_sessionmaker(engine: Engine) -> sessionmaker[Session]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_config.py`:

```python
def test_relay_settings_carry_the_values_the_spec_fixes() -> None:
    settings = Settings()
    assert settings.kafka_topic == "orders.events"
    assert settings.relay_batch_size == 100
    assert settings.relay_lease_seconds == 60
    assert settings.kafka_delivery_timeout_ms == 30_000


def test_a_delivery_timeout_at_or_above_the_lease_is_refused() -> None:
    # Spec 7.1: a publish still in flight when the lease expires is reclaimed by
    # another relay and published twice. The config must not be able to say that.
    with pytest.raises(ValidationError, match="delivery_timeout"):
        Settings(kafka_delivery_timeout_ms=60_000, relay_lease_seconds=60)
```

Add `from pydantic import ValidationError` and `from outboxexpress.shared.config import Settings` to the imports if they are not already there.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `'Settings' object has no attribute 'kafka_topic'`.

- [ ] **Step 3: Add the settings**

In `src/outboxexpress/shared/config.py`, replace the `# No Kafka settings here...` comment and add the fields plus the validator:

```python
from typing import Self

from pydantic import model_validator
```

```python
    database_url: str = "postgresql+psycopg://outbox:outbox@localhost:5432/outbox"
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    log_level: str = "INFO"
    log_json: bool = True

    # The api has no Kafka client and no Kafka configuration (spec 4). These serve
    # the relay, and from M3 the notifier.
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "orders.events"
    kafka_delivery_timeout_ms: int = 30_000

    relay_batch_size: int = 100
    relay_lease_seconds: int = 60
    relay_poll_interval_seconds: float = 0.5

    @model_validator(mode="after")
    def _delivery_must_time_out_inside_the_lease(self) -> Self:
        """Spec 7.1's hard constraint. Tests shrink both, so it is checked, not pinned."""
        if self.kafka_delivery_timeout_ms >= self.relay_lease_seconds * 1000:
            raise ValueError(
                f"kafka_delivery_timeout_ms ({self.kafka_delivery_timeout_ms}) must be "
                f"below relay_lease_seconds ({self.relay_lease_seconds}s): a publish "
                "outliving its lease is reclaimed by another relay and published twice"
            )
        return self
```

- [ ] **Step 4: Add the sync session factory**

In `src/outboxexpress/shared/db.py`, add beside `create_sessionmaker`:

```python
from sqlalchemy.orm import Session, sessionmaker
```

```python
def create_sync_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    # No expire_on_commit=False here, unlike the async factory: the relay reads Rows
    # from RETURNING rather than ORM instances, so expiry has nothing to reload.
    return sessionmaker(engine)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
uv run ruff check . && uv run pyright
git add src/outboxexpress/shared/config.py src/outboxexpress/shared/db.py tests/unit/test_config.py
git commit -m "feat: add relay settings and a sync session factory"
```

---

### Task 3: Claim and mark — the relay's outbox transitions

**Files:**
- Create: `src/outboxexpress/relay/__init__.py`
- Create: `src/outboxexpress/relay/outbox.py`
- Create: `tests/pending_events.py`
- Modify: `tests/conftest.py` (add the `relay_sessions` fixture)
- Test: `tests/unit/test_relay_outbox.py`

**Interfaces:**
- Consumes: `Outbox`, `OutboxStatus`, `new_id`, `utcnow` from `shared.models`; `create_sync_sessionmaker` from Task 2.
- Produces: `ClaimedEvent(event_id: UUID, aggregate_id: str, event_type: str, payload: dict[str, Any])` frozen dataclass; `claim_batch(session: Session, *, instance_id: str, batch_size: int, lease_seconds: int) -> list[ClaimedEvent]`; `mark_published(session: Session, event_ids: Sequence[UUID], *, instance_id: str) -> int`. For tests: `relay_sessions` fixture → `sessionmaker[Session]` (conftest), and `write_pending_events(sessions, *, count, aggregate_id=None) -> list[UUID]` (`tests/pending_events.py`).

- [ ] **Step 1: Add the session fixture and the row-writing helper**

Task 5 needs both. They are split by how a test reaches them, which is pytest's own rule: fixtures are injected from `conftest.py` and never imported, while a plain function belongs in a module a test can import. The repo already follows this — the four `from conftest import` sites in `tests/` take only `RunAsync` and `SessionFactory`, and use them purely as annotations.

In `tests/conftest.py`, add the fixture:

```python
from sqlalchemy.orm import Session, sessionmaker

from outboxexpress.shared.db import create_sync_sessionmaker
```

```python
@pytest.fixture
def relay_sessions(sync_engine: Engine) -> sessionmaker[Session]:
    """Sessions shaped the way the relay opens them: synchronous, one per cycle."""
    return create_sync_sessionmaker(sync_engine)
```

Create `tests/pending_events.py`:

```python
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
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/test_relay_outbox.py`:

```python
"""The relay's two state transitions over the outbox table."""

from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outboxexpress.relay.outbox import claim_batch, mark_published
from outboxexpress.shared.models import utcnow
from pending_events import write_pending_events

LEASE = 60


def row_state(engine: Engine, event_id: UUID) -> dict[str, Any]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    "SELECT status, attempts, locked_by, locked_until, published_at "
                    "FROM outbox WHERE id = :id"
                ),
                {"id": event_id},
            )
            .mappings()
            .one()
        )


def test_a_claim_takes_the_lease_and_counts_the_attempt(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claimed = claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == [event_id]
    state = row_state(sync_engine, event_id)
    assert state["status"] == "publishing"
    assert state["attempts"] == 1
    assert state["locked_by"] == "relay-1"
    assert state["locked_until"] > utcnow()


def test_a_claim_returns_the_event_the_publisher_needs(relay_sessions: sessionmaker[Session]) -> None:
    write_pending_events(relay_sessions, count=1, aggregate_id="order-7")

    with relay_sessions() as session:
        (event,) = claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)

    assert event.aggregate_id == "order-7"
    assert event.event_type == "OrderCreated"
    assert event.payload == {"sequence": 0}


def test_a_claim_stops_at_the_batch_size_and_takes_the_oldest_first(
    relay_sessions: sessionmaker[Session],
) -> None:
    written = write_pending_events(relay_sessions, count=5)

    with relay_sessions() as session:
        claimed = claim_batch(session, instance_id="relay-1", batch_size=3, lease_seconds=LEASE)

    assert [event.event_id for event in claimed] == written[:3]


def test_a_row_not_yet_due_is_left_alone(relay_sessions: sessionmaker[Session], sync_engine: Engine) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)
    with sync_engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox SET next_attempt_at = now() + interval '1 hour' WHERE id = :id"),
            {"id": event_id},
        )

    with relay_sessions() as session:
        assert claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE) == []


def test_two_relays_claiming_at_once_get_disjoint_rows(relay_sessions: sessionmaker[Session]) -> None:
    # The SKIP LOCKED contract. I3 proves it across two processes in plan 2; here it is
    # the unit behaviour of the statement itself.
    write_pending_events(relay_sessions, count=6)

    with relay_sessions() as first, relay_sessions() as second:
        one = claim_batch(first, instance_id="relay-1", batch_size=3, lease_seconds=LEASE)
        two = claim_batch(second, instance_id="relay-2", batch_size=3, lease_seconds=LEASE)

    assert len(one) == 3 and len(two) == 3
    assert {event.event_id for event in one}.isdisjoint({event.event_id for event in two})


def test_marking_published_clears_the_lease(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
        assert mark_published(session, [event_id], instance_id="relay-1") == 1

    state = row_state(sync_engine, event_id)
    assert state["status"] == "published"
    assert state["published_at"] is not None
    assert state["locked_by"] is None
    assert state["locked_until"] is None


def test_a_relay_cannot_mark_a_row_another_relay_holds(
    relay_sessions: sessionmaker[Session], sync_engine: Engine
) -> None:
    # The slow relay whose lease expired must not overwrite the outcome of the relay
    # that took the row from it. Guarding on locked_by is what makes that impossible.
    (event_id,) = write_pending_events(relay_sessions, count=1)

    with relay_sessions() as session:
        claim_batch(session, instance_id="relay-1", batch_size=10, lease_seconds=LEASE)
        assert mark_published(session, [event_id], instance_id="relay-2") == 0

    assert row_state(sync_engine, event_id)["status"] == "publishing"


def test_marking_nothing_touches_nothing(relay_sessions: sessionmaker[Session]) -> None:
    with relay_sessions() as session:
        assert mark_published(session, [], instance_id="relay-1") == 0
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.relay'`.

- [ ] **Step 4: Write the module**

Create `src/outboxexpress/relay/__init__.py` empty, then `src/outboxexpress/relay/outbox.py`:

```python
"""Every state transition the relay makes to the outbox table.

The claim and the mark are two halves of one sequence over one table, so they share a
module and a reason to change; the reclaim and the dead-letter join them in later
plans. Each function owns its transaction and holds it for a single statement -- no
lock in this file is ever held across the network call in ``publisher``.
"""

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


def mark_published(
    session: Session, event_ids: Sequence[UUID], *, instance_id: str
) -> int:
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_outbox.py -v`
Expected: PASS, 8 tests.

- [ ] **Step 6: Commit**

```bash
uv run ruff check . && uv run pyright
git add src/outboxexpress/relay tests/pending_events.py tests/conftest.py tests/unit/test_relay_outbox.py
git commit -m "feat: claim outbox rows under a lease and mark them published"
```

---

### Task 4: The producer

**Files:**
- Create: `src/outboxexpress/relay/publisher.py`
- Create: `tests/topic_reader.py`
- Test: `tests/unit/test_relay_publisher.py`

**Interfaces:**
- Consumes: `ClaimedEvent` from Task 3; `Settings` from Task 2.
- Produces: `producer_config(settings: Settings) -> dict[str, Any]`; `create_producer(settings: Settings) -> Producer`; `flush_timeout_seconds(settings: Settings) -> float`; `PublishOutcome(event_id: UUID, error: KafkaError | None)` frozen dataclass; `publish_batch(producer: Producer, events: Sequence[ClaimedEvent], *, topic: str, timeout_seconds: float) -> list[PublishOutcome]`. For tests, in `tests/topic_reader.py`: `read_events(broker, topic, *, expected, timeout=READ_TIMEOUT) -> list[Message]`, plus `READ_TIMEOUT` and `QUIET_TIMEOUT`.

- [ ] **Step 1: Add the topic reader**

Task 5 reads the topic back too, and how to reach a topic from its start is one fact about the broker, not one per test file. It is a plain function rather than a fixture, so it gets a module of its own rather than joining `conftest.py`.

Create `tests/topic_reader.py`:

```python
"""Reading a topic back from the start, for tests that assert what was published."""

from uuid import uuid4

from confluent_kafka import Consumer, Message

READ_TIMEOUT = 30.0
# Long enough that a message in flight would have landed, short enough that asserting
# an empty topic does not cost half a minute.
QUIET_TIMEOUT = 5.0


def read_events(
    broker: str, topic: str, *, expected: int, timeout: float = READ_TIMEOUT
) -> list[Message]:
    """Read up to ``expected`` messages from the start of ``topic``.

    Returns early with fewer when the topic goes quiet, so it serves both "these
    arrived" and "nothing arrived" -- the latter with a short ``timeout``. A throwaway
    group id per call is what puts every read back at offset zero.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"test-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        messages: list[Message] = []
        while len(messages) < expected:
            message = consumer.poll(timeout)
            if message is None:
                break
            assert message.error() is None, message.error()
            messages.append(message)
        return messages
    finally:
        consumer.close()
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/test_relay_publisher.py`:

```python
"""The producer half of the cycle. No Postgres in this file."""

import json

from outboxexpress.relay.outbox import ClaimedEvent
from outboxexpress.relay.publisher import (
    create_producer,
    flush_timeout_seconds,
    producer_config,
    publish_batch,
)
from outboxexpress.shared.config import Settings
from outboxexpress.shared.models import new_id

from confluent_kafka import Message
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
    assert value is not None  # Message.value() is bytes | None
    assert json.loads(value) == {"sequence": 0}
    assert header(message, "event_id") == str(event.event_id).encode()
    assert header(message, "event_type") == b"OrderCreated"


def test_one_aggregate_lands_on_one_partition_in_produce_order(
    broker: str, topic: str
) -> None:
    # I5. The key puts them on one partition; enable.idempotence is what stops
    # librdkafka's own retries from reordering them once they are there.
    events = [an_event("order-7", sequence) for sequence in range(5)]
    settings = Settings(kafka_bootstrap_servers=broker)
    producer = create_producer(settings)

    publish_batch(
        producer, events, topic=topic, timeout_seconds=flush_timeout_seconds(settings)
    )

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
```

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_publisher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.relay.publisher'`.

- [ ] **Step 4: Write the module**

Create `src/outboxexpress/relay/publisher.py`:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_publisher.py -v`
Expected: PASS, 5 tests. The unreachable-broker test takes about two seconds — that is its shortened `delivery.timeout.ms` elapsing.

- [ ] **Step 6: Commit**

```bash
uv run ruff check . && uv run pyright
git add src/outboxexpress/relay/publisher.py tests/topic_reader.py tests/unit/test_relay_publisher.py
git commit -m "feat: publish claimed events with acks=all and per-event delivery reports"
```

---

### Task 5: The cycle and its failure-injection seams

**Files:**
- Create: `src/outboxexpress/relay/hooks.py`
- Create: `src/outboxexpress/relay/loop.py`
- Test: `tests/unit/test_relay_loop.py`

**Interfaces:**
- Consumes: `claim_batch`, `mark_published` from Task 3; `create_producer`, `publish_batch`, `flush_timeout_seconds` from Task 4; `Settings` from Task 2; the `relay_sessions` fixture, plus `write_pending_events` and `read_events`/`QUIET_TIMEOUT` from their test modules.
- Produces: `new_instance_id() -> str`; `run_once(session: Session, producer: Producer, *, instance_id: str, settings: Settings) -> int` returning the number of rows marked published; `hooks.after_claim()`, `hooks.after_publish_before_mark()`, `hooks.after_mark()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_relay_loop.py`:

```python
"""One full cycle, including the crash windows spec 6.2 says cannot be closed."""

from collections.abc import Sequence

import pytest
from confluent_kafka import KafkaError, Producer
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from outboxexpress.relay import hooks, loop
from outboxexpress.relay.outbox import ClaimedEvent
from outboxexpress.relay.publisher import PublishOutcome, create_producer
from outboxexpress.shared.config import Settings

from pending_events import write_pending_events
from topic_reader import QUIET_TIMEOUT, read_events


class Boom(Exception):
    """Stands in for the relay process dying at a named seam."""


@pytest.fixture
def settings(broker: str, topic: str) -> Settings:
    return Settings(kafka_bootstrap_servers=broker, kafka_topic=topic)


def statuses(engine: Engine) -> list[str]:
    with engine.connect() as connection:
        return list(
            connection.scalars(text("SELECT status FROM outbox ORDER BY next_attempt_at, id"))
        )


def test_a_cycle_publishes_every_claimed_row_and_retires_it(
    relay_sessions: sessionmaker[Session], sync_engine: Engine, settings: Settings, broker: str
) -> None:
    write_pending_events(relay_sessions, count=3)
    producer = create_producer(settings)

    with relay_sessions() as session:
        marked = loop.run_once(
            session, producer, instance_id="relay-1", settings=settings
        )

    assert marked == 3
    assert statuses(sync_engine) == ["published"] * 3
    assert len(read_events(broker, settings.kafka_topic, expected=3)) == 3


def test_an_empty_outbox_retires_nothing(
    relay_sessions: sessionmaker[Session], settings: Settings
) -> None:
    producer = create_producer(settings)

    with relay_sessions() as session:
        assert loop.run_once(session, producer, instance_id="relay-1", settings=settings) == 0


def test_a_crash_between_claim_and_publish_leaves_the_row_claimed(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    broker: str,
) -> None:
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_claim", _raise)
    producer = create_producer(settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=settings)

    # Nothing published, and the row still holds its lease. Recovering it is the
    # reclaimer's job in plan 2 -- until then this is where the row stops.
    assert statuses(sync_engine) == ["publishing"]
    assert read_events(broker, settings.kafka_topic, expected=1, timeout=QUIET_TIMEOUT) == []


def test_a_crash_after_the_ack_leaves_kafka_ahead_of_postgres(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    broker: str,
) -> None:
    # Spec 6.2's one irreducible window, asserted rather than hoped for: the event is
    # on the topic and the row still says publishing. The lease will expire, the row
    # will be republished, and the consumer's dedup absorbs it. This is why the
    # guarantee is at-least-once.
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_publish_before_mark", _raise)
    producer = create_producer(settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=settings)

    assert statuses(sync_engine) == ["publishing"]
    assert len(read_events(broker, settings.kafka_topic, expected=1)) == 1


def test_the_mark_hook_runs_after_the_row_is_retired(
    relay_sessions: sessionmaker[Session],
    sync_engine: Engine,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_pending_events(relay_sessions, count=1)
    monkeypatch.setattr(hooks, "after_mark", _raise)
    producer = create_producer(settings)

    with relay_sessions() as session, pytest.raises(Boom):
        loop.run_once(session, producer, instance_id="relay-1", settings=settings)

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

    # The rejected row keeps its lease and its claim, and only the reclaimer in plan 2
    # brings it back. What must never happen is it being retired alongside its sibling.
    assert marked == 1
    assert statuses(sync_engine) == ["published", "publishing"]


def test_instance_ids_distinguish_two_processes_on_one_host() -> None:
    assert loop.new_instance_id() != loop.new_instance_id()


def _raise() -> None:
    raise Boom
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_relay_loop.py -v`
Expected: FAIL — `ImportError: cannot import name 'hooks' from 'outboxexpress.relay'`.

- [ ] **Step 3: Write the hooks**

Create `src/outboxexpress/relay/hooks.py`:

```python
"""Failure-injection seams (spec 9.1).

A SIGKILL cannot reliably land between a Kafka ack and a Postgres commit -- the window
is microseconds wide. These no-ops give tests a place to raise instead, which turns
"crash at exactly the worst moment" into an assertion.

``loop`` calls these through the module (``hooks.after_claim()``), never by importing
the names: monkeypatching rebinds the module attribute, and an imported name would
still point at the original function.
"""


def after_claim() -> None:
    """The rows are leased; nothing is published yet."""


def after_publish_before_mark() -> None:
    """Kafka has the events; Postgres still says publishing. Spec 6.2's open window."""


def after_mark() -> None:
    """The rows are retired."""
```

- [ ] **Step 4: Write the loop**

Create `src/outboxexpress/relay/loop.py`:

```python
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
from .outbox import claim_batch, mark_published
from .publisher import flush_timeout_seconds, publish_batch

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

    delivered = [outcome.event_id for outcome in outcomes if outcome.error is None]
    marked = mark_published(session, delivered, instance_id=instance_id)
    hooks.after_mark()

    log.info(
        "relay_cycle",
        locked_by=instance_id,
        claimed=len(claimed),
        published=marked,
        failed=len(claimed) - marked,
    )
    return marked
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_relay_loop.py -v`
Expected: PASS, 7 tests.

- [ ] **Step 6: Run the whole suite**

Run: `uv run pytest && uv run ruff check . && uv run pyright`
Expected: every M1 test still passes, and pyright reports no errors. The suite now starts two containers, so it is slower than before.

- [ ] **Step 7: Commit**

```bash
git add src/outboxexpress/relay/hooks.py src/outboxexpress/relay/loop.py tests/unit/test_relay_loop.py
git commit -m "feat: run one relay cycle with failure-injection seams"
```

---

## Verification

Before calling this plan done:

```bash
make test    # the whole suite, Postgres and Kafka via testcontainers
make lint    # ruff + pyright strict
```

Four things below were checked while this plan was written rather than reasoned about,
so if they misbehave during implementation, suspect the surrounding fixture first:

- **The two statements in `outbox.py` ran against a real Postgres 18.** The claim
  returns rows in write order, two concurrent claims come back disjoint, the
  `locked_by` guard refuses a foreign relay's mark, and a published row comes out with
  `attempts=1` and a cleared lease.
- **§8.2's producer config is accepted by librdkafka verbatim** — `delivery.timeout.ms`
  and `compression.type` are both valid keys, and `acks=all` is required to be
  consistent with `enable.idempotence` rather than merely allowed: librdkafka fails
  producer construction on an incompatible `acks`.
- **`str` keys and `str` header values are accepted**, arriving as UTF-8 bytes, which is
  what the assertions on `message.key()` and the headers expect.
- **`pyright` strict and `ruff` pass on all four relay modules** plus the Task 2
  settings, including `Producer.produce(..., on_delivery=...)` and the `Row` unpacking
  in `claim_batch`.
- **Delivery callbacks run on the calling thread**, served from `flush()`, never from
  librdkafka's background thread -- which is why `publish_batch` writes its `verdicts`
  dict without a lock.
- **The container matches the KRaft recipe in the installed testcontainers 4.15.0**:
  `sh {script}` rather than direct execution, no file mode, and `Kafka Server started`
  as the readiness marker. Do not move that marker earlier -- the unfencing line is
  logged before the listener accepts connections.
- **The delivery-verdict filter in `run_once` was mutation-checked.** Replacing it with
  an unfiltered `[outcome.event_id for outcome in outcomes]` leaves 58 of 59 tests
  green; only `test_a_cycle_retires_only_the_events_the_broker_confirmed` catches it.
  Treat that test as load-bearing rather than redundant.

The I5 ordering claim comes from librdkafka's own guarantees: "with Idempotent Producer
enabled there is no risk of reordering despite `max.in.flight` > 1 (capped at 5)"
([INTRODUCTION.md](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md)).
The key puts one aggregate on one partition; idempotence is what keeps it ordered there.

What a reviewer should be able to see from the suite alone: a committed order's event reaching `orders.events` keyed by its order id, one aggregate's events arriving in order on one partition, and the ack-before-mark window asserted instead of assumed. What they will not see yet, by design: any row recovering from a failure. That is plan 2.

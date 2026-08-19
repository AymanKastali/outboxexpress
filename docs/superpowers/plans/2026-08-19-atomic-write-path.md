# Atomic Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write an order together with its `OrderCreated` event in a single Postgres transaction, once per `Idempotency-Key`.

**Architecture:** One function, `place_order`, opens one transaction and inserts three rows: `orders`, `outbox`, `idempotency_keys`. Concurrency is resolved by the `idempotency_keys` primary key — the loser of a race catches `IntegrityError` and replays the winner's stored response — so there is no application-level locking.

**Tech Stack:** Python 3.14 · SQLAlchemy 2.0 (async) · `psycopg[binary]` 3 · PostgreSQL 18 · pytest + testcontainers

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§6.1 write path, §5.3 idempotency, §9.2 invariants I2 and I7)

## Global Constraints

- **Nothing here imports a Kafka client or mentions a broker.** The write path's durability is one commit (spec §4).
- **`outbox.id` IS the `event_id`** (spec §5.5) — one UUID, generated once.
- **A replay returns the stored `response_code` and `response_body` verbatim** (spec §11).
- **No `pytest-asyncio`** (spec §3.6) — async tests call `asyncio.run` via the `run_async` fixture.
- Order status is always `'CREATED'` (spec §2).

## Out of Scope

- HTTP — no FastAPI route, no request parsing, no status codes over the wire (plan 3)
- Docker, compose, and the Makefile (plan 4)
- The relay, the outbox `status` transitions past `pending`, and anything that reads the outbox (M2)
- Purging or archiving `published` rows (spec §12, deliberately deferred)

## Plan Queue

This work is split across small plans. **Only plan 2 is written.**

1. ✅ **Shipped** — shared foundations: settings, logging, models, migration, event contract, request fingerprint (6 commits on `m1-foundation-and-write-path`)
2. **[this plan]** Write an order and its event atomically — `2026-08-19-atomic-write-path.md`
3. Serve it over HTTP as idempotent `POST /orders` — write after plan 2 ships
4. Run the Postgres-only stack under compose — write after plan 3 ships

---

### Task 1: `place_order` writes three rows in one transaction

**Files:**
- Create: `src/outboxexpress/api/service.py`
- Test: `tests/unit/test_service.py`

**Interfaces:**
- Consumes: `NewOrder`, `request_fingerprint`, `load_key`, `IdempotencyConflict` (from `api/`); `Order`, `Outbox`, `IdempotencyKey`, `new_id`, `utcnow`, `ORDER_STATUS_CREATED`, `AGGREGATE_TYPE_ORDER` (from `shared/models.py`); `OrderCreatedV1`, `OrderSnapshot` (from `shared/events.py`)
- Produces: `StoredResponse(status_code: int, body: dict)`; `async place_order(session, *, idempotency_key: str, request: NewOrder) -> StoredResponse`; `async _write(session, idempotency_key, request) -> IdempotencyKey`

- [ ] **Step 1: Write the failing test**

```python
from sqlalchemy import Engine, text

from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


def counts(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        return {
            table: connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            for table in ("orders", "outbox", "idempotency_keys")
        }


def test_one_call_writes_exactly_one_order_and_one_event(run_async, sync_engine):
    async def scenario(factory):
        async with factory() as session:
            return await place_order(session, idempotency_key="k-1", request=REQUEST)

    stored = run_async(scenario)
    assert stored.status_code == 201
    assert counts(sync_engine) == {"orders": 1, "outbox": 1, "idempotency_keys": 1}


def test_the_outbox_row_carries_the_event_and_the_order_as_its_key(run_async, sync_engine):
    async def scenario(factory):
        async with factory() as session:
            return await place_order(session, idempotency_key="k-1", request=REQUEST)

    stored = run_async(scenario)
    with sync_engine.connect() as connection:
        row = connection.execute(
            text("SELECT id, aggregate_type, aggregate_id, event_type, payload, status FROM outbox")
        ).one()
    assert row.aggregate_type == "order"
    assert row.aggregate_id == stored.body["order_id"]
    assert row.event_type == "OrderCreated"
    assert row.status == "pending"
    # spec 5.5: outbox.id IS the event_id
    assert row.payload["event_id"] == str(row.id)
    assert row.payload["order"]["id"] == stored.body["order_id"]
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_service.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.service'`

- [ ] **Step 3: Write `src/outboxexpress/api/service.py`**

```python
"""The write path. One transaction, three rows, no network call."""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from outboxexpress.shared.events import OrderCreatedV1, OrderSnapshot
from outboxexpress.shared.logging import get_logger
from outboxexpress.shared.models import (
    AGGREGATE_TYPE_ORDER,
    ORDER_STATUS_CREATED,
    IdempotencyKey,
    Order,
    Outbox,
    new_id,
    utcnow,
)

from .idempotency import request_fingerprint
from .schemas import NewOrder

log = get_logger(__name__)

CREATED = 201


@dataclass(frozen=True)
class StoredResponse:
    status_code: int
    body: dict


async def place_order(
    session: AsyncSession, *, idempotency_key: str, request: NewOrder
) -> StoredResponse:
    """Create an order and its event atomically."""
    async with session.begin():
        record = await _write(session, idempotency_key, request)
    # Readable after commit only because the sessionmaker sets expire_on_commit=False.
    log.info("order_committed", order_id=str(record.order_id))
    return StoredResponse(record.response_code, record.response_body)


async def _write(session: AsyncSession, idempotency_key: str, request: NewOrder) -> IdempotencyKey:
    now = utcnow()
    order = Order(
        id=new_id(),
        customer_email=request.customer_email,
        item_sku=request.item_sku,
        quantity=request.quantity,
        status=ORDER_STATUS_CREATED,
        created_at=now,
    )
    event = OrderCreatedV1(
        event_id=new_id(),
        occurred_at=now,
        order=OrderSnapshot(
            id=order.id,
            customer_email=order.customer_email,
            item_sku=order.item_sku,
            quantity=order.quantity,
            created_at=order.created_at,
        ),
    )
    outbox = Outbox(
        id=event.event_id,  # spec 5.5 — one UUID crosses every boundary
        aggregate_type=AGGREGATE_TYPE_ORDER,
        aggregate_id=str(order.id),  # becomes the Kafka partition key in M2
        event_type=event.event_type,
        payload=event.model_dump(mode="json"),
        created_at=now,
    )
    record = IdempotencyKey(
        key=idempotency_key,
        request_hash=request_fingerprint(request),
        order_id=order.id,
        response_code=CREATED,
        response_body={
            "order_id": str(order.id),
            "status": order.status,
            "created_at": order.created_at.isoformat(),
        },
    )
    log.info("order_written", order_id=str(order.id), event_id=str(event.event_id))
    # The order must land before the row whose foreign key references it. SQLAlchemy
    # sorts a flush by *relationship* dependency, and there is no relationship here --
    # only a raw ForeignKey column -- so the ordering is stated explicitly.
    session.add(order)
    await session.flush()
    session.add_all([outbox, record])
    return record
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_service.py -q`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api/service.py tests/unit/test_service.py
git commit -m "feat: write an order and its event in one transaction"
```

---

### Task 2: Replay and conflict on a reused key

**Files:**
- Modify: `src/outboxexpress/api/service.py`
- Test: `tests/unit/test_service.py`

**Interfaces:**
- Consumes: `load_key`, `IdempotencyConflict` from `api/idempotency.py`
- Produces: `_replay(record: IdempotencyKey, request: NewOrder) -> StoredResponse`; `place_order` now returns the stored response for a repeated key and raises `IdempotencyConflict` when the body differs

- [ ] **Step 1: Write the failing test — append to `tests/unit/test_service.py`**

```python
import pytest

from outboxexpress.api.idempotency import IdempotencyConflict


def test_replaying_a_key_returns_the_stored_response_byte_for_byte(run_async, sync_engine):
    async def scenario(factory):
        async with factory() as session:
            first = await place_order(session, idempotency_key="k-1", request=REQUEST)
        async with factory() as session:
            second = await place_order(session, idempotency_key="k-1", request=REQUEST)
        return first, second

    first, second = run_async(scenario)
    assert first.body == second.body
    assert first.status_code == second.status_code == 201
    assert counts(sync_engine) == {"orders": 1, "outbox": 1, "idempotency_keys": 1}


def test_reusing_a_key_with_a_different_body_is_a_conflict(run_async):
    async def scenario(factory):
        async with factory() as session:
            await place_order(session, idempotency_key="k-1", request=REQUEST)
        async with factory() as session:
            await place_order(
                session,
                idempotency_key="k-1",
                request=NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=99),
            )

    with pytest.raises(IdempotencyConflict):
        run_async(scenario)
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_service.py -q`
Expected: FAIL — the second call inserts a duplicate key and raises `IntegrityError`, not `IdempotencyConflict`

- [ ] **Step 3: Add the lookup and replay to `service.py`**

Change the import line to bring in the two extra names:

```python
from .idempotency import IdempotencyConflict, load_key, request_fingerprint
```

Replace the body of `place_order` with:

```python
async def place_order(
    session: AsyncSession, *, idempotency_key: str, request: NewOrder
) -> StoredResponse:
    """Create an order and its event atomically, or replay a previous identical request."""
    # The lookup sits inside the transaction: a read before session.begin() would open
    # an implicit one, and begin() would then raise "a transaction is already begun".
    async with session.begin():
        seen = await load_key(session, idempotency_key)
        if seen is not None:
            return _replay(seen, request)
        record = await _write(session, idempotency_key, request)
    log.info("order_committed", order_id=str(record.order_id))
    return StoredResponse(record.response_code, record.response_body)
```

And append:

```python
def _replay(record: IdempotencyKey, request: NewOrder) -> StoredResponse:
    if record.request_hash != request_fingerprint(request):
        raise IdempotencyConflict(record.key)
    return StoredResponse(record.response_code, record.response_body)
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_service.py -q`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api/service.py tests/unit/test_service.py
git commit -m "feat: replay a stored response for a reused idempotency key"
```

---

### Task 3: Invariant I2 — no orphan events

**Files:**
- Test: `tests/invariants/test_i2_no_orphan_events.py`

**Interfaces:**
- Consumes: `place_order`, `_write`, `NewOrder`, the `run_async` and `sync_engine` fixtures
- Produces: nothing importable

- [ ] **Step 1: Write the test**

```python
"""I2: no event exists whose order is not committed, and no order lacks its event."""

import pytest
from sqlalchemy import Engine, text

from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import _write, place_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


def table_counts(engine: Engine) -> tuple[int, int]:
    with engine.connect() as connection:
        return (
            connection.execute(text("SELECT count(*) FROM orders")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM outbox")).scalar_one(),
        )


def test_a_failed_transaction_leaves_neither_an_order_nor_an_event(run_async, sync_engine):
    class Boom(Exception):
        pass

    async def scenario(factory):
        async with factory() as session:
            with pytest.raises(Boom):
                async with session.begin():
                    await _write(session, "k-1", REQUEST)
                    # Force all three INSERTs to the server, so the rollback below is
                    # undoing real rows rather than discarding a pending unit of work.
                    await session.flush()
                    raise Boom  # a crash after the inserts, before COMMIT

    run_async(scenario)
    assert table_counts(sync_engine) == (0, 0)


def test_every_committed_order_has_exactly_one_matching_event(run_async, sync_engine):
    async def scenario(factory):
        for index in range(5):
            async with factory() as session:
                await place_order(session, idempotency_key=f"k-{index}", request=REQUEST)

    run_async(scenario)
    assert table_counts(sync_engine) == (5, 5)

    with sync_engine.connect() as connection:
        orphans = connection.execute(
            text(
                "SELECT count(*) FROM outbox o "
                "LEFT JOIN orders ord ON ord.id::text = o.aggregate_id "
                "WHERE ord.id IS NULL"
            )
        ).scalar_one()
        eventless = connection.execute(
            text(
                "SELECT count(*) FROM orders ord "
                "LEFT JOIN outbox o ON o.aggregate_id = ord.id::text "
                "WHERE o.id IS NULL"
            )
        ).scalar_one()
    assert orphans == 0
    assert eventless == 0
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/invariants/test_i2_no_orphan_events.py -v`
Expected: 2 passed

- [ ] **Step 3: Commit**

```bash
git add tests/invariants/test_i2_no_orphan_events.py
git commit -m "test: prove invariant I2, no orphan events"
```

---

### Task 4: Invariant I7 — edge idempotency under real concurrency

**Files:**
- Modify: `src/outboxexpress/api/service.py`
- Test: `tests/invariants/test_i7_edge_idempotency.py`

**Interfaces:**
- Consumes: `place_order`, `NewOrder`, the `run_async` and `sync_engine` fixtures
- Produces: `place_order` now survives a lost race by catching `IntegrityError` and replaying the winner

- [ ] **Step 1: Write the test**

```python
"""I7: N concurrent identical posts create exactly one order and one event."""

import asyncio

from sqlalchemy import Engine, text

from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)
CONCURRENCY = 8


def test_concurrent_posts_with_one_key_create_one_order(run_async, sync_engine: Engine):
    async def scenario(factory):
        async def attempt():
            # A separate session per caller: a real race in Postgres, not a simulated
            # one. The losers block on the idempotency_keys primary key.
            async with factory() as session:
                return await place_order(session, idempotency_key="k-1", request=REQUEST)

        return await asyncio.gather(*(attempt() for _ in range(CONCURRENCY)))

    responses = run_async(scenario)

    assert len(responses) == CONCURRENCY
    assert {response.status_code for response in responses} == {201}
    # Every caller — winner and losers alike — sees the same body.
    assert len({response.body["order_id"] for response in responses}) == 1

    with sync_engine.connect() as connection:
        for table in ("orders", "outbox", "idempotency_keys"):
            count = connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            assert count == 1, f"{table} has {count} rows, expected 1"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/invariants/test_i7_edge_idempotency.py -v`
Expected: FAIL — seven of the eight callers raise `IntegrityError` on the duplicate key

- [ ] **Step 3: Handle the lost race in `service.py`**

Add the import:

```python
from sqlalchemy.exc import IntegrityError
```

and wrap the write in `place_order`:

```python
    try:
        async with session.begin():
            record = await _write(session, idempotency_key, request)
    except IntegrityError:
        # Another request committed this key first. It blocked us on the primary key
        # until it committed, so by the time we get here the winner is durably visible.
        winner = await load_key(session, idempotency_key)
        if winner is None:
            raise  # a different constraint failed — do not swallow it
        log.info("idempotent_race_lost", idempotency_key=idempotency_key)
        return _replay(winner, request)
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/invariants/test_i7_edge_idempotency.py -v`
Expected: 1 passed. A connection-pool timeout here means the async engine is shared across
event loops — check that `run_async` builds it inside `main()`.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -q`
Expected: all passed

- [ ] **Step 6: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api/service.py tests/invariants/test_i7_edge_idempotency.py
git commit -m "test: prove invariant I7, edge idempotency under concurrency"
```

---

## Definition of done

- [ ] `uv run pytest -q` green
- [ ] `uv run ruff check . && uv run ruff format --check .` clean
- [ ] Nothing under `src/` imports a Kafka client
- [ ] I2 and I7 both pass against a real Postgres

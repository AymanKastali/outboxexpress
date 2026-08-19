# Read Order Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve a committed order back over HTTP as `GET /orders/{id}`.

**Architecture:** A new `queries.py` holds the read path, kept separate from `service.py` because the write path's one reason to change is its transaction and a read has no transaction to own. `load_order` raises `OrderNotFound` rather than returning `None`, so the route stays free of branches and the 404 is an exception handler beside the 400, 409 and 503 that already live in `errors.py`.

**Tech Stack:** Python 3.14 · FastAPI 0.141 · SQLAlchemy 2.0 (async) · httpx 0.28 (`ASGITransport`) · pytest + testcontainers

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§11 API contract)

## Global Constraints

- **`GET /orders/{id}` → `200` / `404`** (spec §11) — the only read endpoint in scope.
- **Failures are handlers, not branches** — routes never name a status code, matching `errors.py`.
- **Nothing here imports a Kafka client.**
- **No `pytest-asyncio`** (spec §3.6) — async tests go through the existing `run_async` fixture.
- **`ruff check .` and `pyright` (strict) stay clean**; line length 100.

## Out of Scope

- Listing, filtering, or paginating orders — the spec names one read endpoint
- Reading `outbox` rows over HTTP — that state is the relay's, not a client's (M2)
- Docker, compose, the Makefile, and the README (plan 5)
- Unifying how the two endpoints spell UTC. `POST` stores `+00:00` (Python `isoformat`); this endpoint emits `Z` (pydantic). Both are RFC 3339 and denote the same instant, and changing the first would alter a **stored** response body — a write-path change.

## Plan Queue

This work is split across small plans. **Only plan 4 is written.**

1. ✅ **Shipped** — shared foundations: settings, logging, models, migration, event contract
2. ✅ **Shipped** — write an order and its event atomically — `2026-08-19-atomic-write-path.md`
3. ✅ **Shipped** — serve the write path as idempotent `POST /orders` — `2026-08-19-http-write-endpoint.md`
4. **[this plan]** Read an order back over HTTP — `2026-08-19-read-order-endpoint.md`
5. Run the Postgres-only stack under compose — write after plan 4 ships

---

### Task 1: The read path — `load_order` and `OrderNotFound`

**Files:**
- Create: `src/outboxexpress/api/queries.py`
- Test: `tests/unit/test_queries.py`

**Interfaces:**
- Consumes: `Order`, `new_id` (from `shared/models.py`); `place_order` (from `api/service.py`) — used by the test to create a row to read back
- Produces: `load_order(session: AsyncSession, order_id: UUID) -> Order` and `OrderNotFound(Exception)` with an `.order_id: UUID` attribute

- [x] **Step 1: Write the failing tests**

Create `tests/unit/test_queries.py`. It reaches a real database through the
existing fixtures, exactly as `tests/unit/test_service.py` does.

```python
from uuid import UUID

import pytest

from conftest import RunAsync, SessionFactory
from outboxexpress.api.queries import OrderNotFound, load_order
from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order
from outboxexpress.shared.models import new_id

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


def test_load_order_returns_the_order_that_was_written(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[str, str, str, int]:
        async with factory() as session:
            stored = await place_order(session, idempotency_key="k-1", request=REQUEST)
        written_id = stored.body["order_id"]
        # A second session, so the read genuinely goes to Postgres rather than
        # finding the instance still in the first session's identity map.
        async with factory() as session:
            order = await load_order(session, UUID(written_id))
            return written_id, str(order.id), order.status, order.quantity

    written_id, loaded_id, status, quantity = run_async(scenario)
    assert loaded_id == written_id
    assert status == "CREATED"
    assert quantity == 2


def test_loading_an_unknown_order_raises(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> None:
        async with factory() as session:
            await load_order(session, new_id())

    with pytest.raises(OrderNotFound):
        run_async(scenario)
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_queries.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.queries'`

- [x] **Step 3: Write the minimal implementation**

Create `src/outboxexpress/api/queries.py`:

```python
"""The read path. No transaction of its own -- a single statement is already atomic."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from outboxexpress.shared.models import Order


class OrderNotFound(Exception):
    """No order with that id."""

    def __init__(self, order_id: UUID) -> None:
        super().__init__(f"No order with id {order_id}")
        self.order_id = order_id


async def load_order(session: AsyncSession, order_id: UUID) -> Order:
    """Return the order, or raise ``OrderNotFound``.

    Raising rather than returning ``None`` keeps the absence decision here: the
    caller asks for an order and gets one, so no route has to branch on a null.
    """
    order = await session.get(Order, order_id)
    if order is None:
        raise OrderNotFound(order_id)
    return order
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_queries.py -v`
Expected: PASS (2 passed)

- [x] **Step 5: Run the linters and the full suite**

Run: `uv run ruff check . && uv run pyright && uv run pytest`
Expected: all clean, 33 passed

- [x] **Step 6: Commit**

```bash
git add src/outboxexpress/api/queries.py tests/unit/test_queries.py
git commit -m "feat: load an order by id, or say it is not there"
```

---

### Task 2: The HTTP surface — `GET /orders/{id}`

**Files:**
- Modify: `src/outboxexpress/api/routes.py` (append the route; add the `UUID`, `load_order` and `OrderResponse` imports)
- Modify: `src/outboxexpress/api/errors.py` (register and define the 404 handler)
- Test: `tests/integration/test_get_order.py`

**Interfaces:**
- Consumes: `load_order`, `OrderNotFound` (Task 1); `SessionDep` (from `api/dependencies.py`); `OrderResponse` (from `api/schemas.py`, already defined and until now used only for OpenAPI)
- Produces: `GET /orders/{order_id}` → `200` with `{"order_id", "status", "created_at"}`, `404` when absent, `400` when the path is not a UUID

- [x] **Step 1: Write the failing tests**

Create `tests/integration/test_get_order.py`.

```python
from datetime import datetime
from typing import Any

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client
from outboxexpress.shared.models import new_id


def test_get_returns_the_order_that_was_posted(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any], dict[str, Any]]:
        async with api_client(factory) as client:
            created = await client.post(
                "/orders", json=VALID_ORDER, headers={"Idempotency-Key": "k-1"}
            )
            fetched = await client.get(f"/orders/{created.json()['order_id']}")
            return fetched.status_code, fetched.json(), created.json()

    status, body, created = run_async(scenario)
    assert status == 200
    assert body["order_id"] == created["order_id"]
    assert body["status"] == "CREATED"
    # Instants, not strings: the stored POST body spells UTC "+00:00" and this
    # endpoint spells it "Z". Same moment, two legal RFC 3339 renderings.
    assert datetime.fromisoformat(body["created_at"]) == datetime.fromisoformat(
        created["created_at"]
    )


def test_an_unknown_order_is_404(run_async: RunAsync) -> None:
    """The body is asserted on purpose.

    Before the route exists FastAPI answers an unknown path with its own 404, so
    a status-only assertion would pass against a route that was never written.
    Only our handler names the id.
    """
    missing_id = str(new_id())

    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any]]:
        async with api_client(factory) as client:
            response = await client.get(f"/orders/{missing_id}")
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 404
    assert missing_id in body["detail"]


def test_a_malformed_order_id_is_400(run_async: RunAsync) -> None:
    """FastAPI validates the path parameter, so a non-UUID never reaches Postgres.
    Drop the ``UUID`` annotation on the route and this returns 404 instead."""

    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            return (await client.get("/orders/not-a-uuid")).status_code

    assert run_async(scenario) == 400
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_get_order.py -v`
Expected: all three FAIL — the first and third get FastAPI's 404 for an
unregistered path, and the second gets `detail == "Not Found"`, which does not
contain the id.

- [x] **Step 3: Add the route**

In `src/outboxexpress/api/routes.py`, extend the imports:

```python
from uuid import UUID

from fastapi import APIRouter, status
from sqlalchemy import text

from .dependencies import IdempotencyKeyDep, SessionDep
from .queries import load_order
from .responses import CanonicalJSONResponse
from .schemas import NewOrder, OrderResponse
from .service import place_order
```

and append below `create_order`:

```python
@router.get("/orders/{order_id}", responses={404: {"description": "No order with that id"}})
async def read_order(session: SessionDep, order_id: UUID) -> OrderResponse:
    # A fresh representation, so `response_model` is right here -- unlike the write
    # path, which must hand back the bytes it stored.
    order = await load_order(session, order_id)
    return OrderResponse(order_id=order.id, status=order.status, created_at=order.created_at)
```

- [x] **Step 4: Add the 404 handler**

In `src/outboxexpress/api/errors.py`, import the exception:

```python
from .idempotency import IdempotencyConflict
from .queries import OrderNotFound
```

register it inside `register_error_handlers`, keeping the list in ascending
status order:

```python
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(OrderNotFound, handle_order_not_found)
    app.add_exception_handler(IdempotencyConflict, handle_idempotency_conflict)
    app.add_exception_handler(OperationalError, handle_database_unavailable)
```

and define the handler immediately after `handle_validation_error`, so the file
keeps reading in registration order:

```python
async def handle_order_not_found(_: Request, exc: Exception) -> JSONResponse:
    """404. Naming the id makes the response useful in a log without a request trace."""
    assert isinstance(exc, OrderNotFound)
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_get_order.py -v`
Expected: PASS (3 passed)

- [x] **Step 6: Run the linters and the full suite**

Run: `uv run ruff check . && uv run pyright && uv run pytest`
Expected: all clean, 36 passed

- [x] **Step 7: Commit**

```bash
git add src/outboxexpress/api/routes.py src/outboxexpress/api/errors.py tests/integration/test_get_order.py
git commit -m "feat: read an order back over HTTP"
```

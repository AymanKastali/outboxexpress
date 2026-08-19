# HTTP Write Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the existing write path over HTTP as an idempotent `POST /orders`.

**Architecture:** A lifespan context manager builds one engine and one sessionmaker for the process and hangs the sessionmaker on `app.state`. A request dependency yields a session *without* opening a transaction, because `place_order` already owns it. The route returns a raw `JSONResponse` built from the `StoredResponse` — never a `response_model` — so a replay returns the stored bytes unchanged. Every documented failure is an exception handler, not a branch in the route.

**Tech Stack:** Python 3.14 · FastAPI 0.141 · uvicorn 0.52 · SQLAlchemy 2.0 (async) · httpx 0.28 (`ASGITransport`) · pytest + testcontainers

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md` (§11 API contract, §6.1 write path)

## Global Constraints

- **A replay returns the stored `response_code` and `response_body` verbatim** (spec §11) — a retrying client cannot tell a replay from the original.
- **`Idempotency-Key` is required**; absent → `400`, not FastAPI's default `422` (spec §11).
- **`GET /healthz` checks Postgres and nothing else** (spec §11) — the API is healthy without a broker.
- **Nothing here imports a Kafka client.**
- **No `pytest-asyncio`** (spec §3.6) — async tests go through the existing `run_async` fixture.

## Out of Scope

- `GET /orders/{id}` — reading an order back is its own deliverable (plan 4)
- Docker, compose, the Makefile, and the README (plan 5)
- Auth, rate limiting, CORS, and pagination — not in the spec
- The relay and anything that reads the `outbox` table (M2)

## Plan Queue

This work is split across small plans. **Only plan 3 is written.**

1. ✅ **Shipped** — shared foundations: settings, logging, models, migration, event contract, request fingerprint
2. ✅ **Shipped** — write an order and its event atomically — `2026-08-19-atomic-write-path.md`
3. **[this plan]** Serve the write path as idempotent `POST /orders` — `2026-08-19-http-write-endpoint.md`
4. Read an order back over HTTP (`GET /orders/{id}`) — write after plan 3 ships
5. Run the Postgres-only stack under compose — write after plan 4 ships

---

### Task 1: App factory, lifespan, session dependency, `GET /healthz`

**Files:**
- Create: `src/outboxexpress/api/dependencies.py`
- Create: `src/outboxexpress/api/routes.py`
- Create: `src/outboxexpress/api/main.py`
- Modify: `src/outboxexpress/shared/config.py` (add `api_host`, `api_port`)
- Modify: `tests/conftest.py` (add the `api_client` helper and the `VALID_ORDER` constant)
- Test: `tests/integration/test_healthz.py`

**Interfaces:**
- Consumes: `create_async_engine_from_settings`, `create_sessionmaker` (from `shared/db.py`); `configure_logging`, `get_settings` (from `shared/`)
- Produces: `get_sessionmaker` and `get_session` (FastAPI dependencies), `SessionDep` and `IdempotencyKeyDep` (annotated aliases), `create_app() -> FastAPI`, module-level `app`, `run() -> None`

- [x] **Step 1: Write the failing test**

In `tests/conftest.py`. The helper goes where the other test plumbing already
lives, so integration tests keep the existing `from conftest import ...` import
style (`tests/integration/` is not a package). Add the imports at the top, put
`VALID_ORDER` beside the existing `PROJECT_ROOT` / `TABLES` constants, and append
`api_client` at the end:

```python
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from outboxexpress.api.dependencies import get_session
from outboxexpress.api.main import create_app


@asynccontextmanager
async def api_client(factory: SessionFactory) -> AsyncGenerator[AsyncClient]:
    """A client wired to the test database.

    ASGITransport does not run lifespan events, so the app never builds its own
    engine here. Overriding the session dependency is enough, and it keeps the
    ``asgi-lifespan`` package out of the dependency list.
    """
    app = create_app()

    async def use_test_session() -> AsyncIterator[AsyncSession]:
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = use_test_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
```

```python
# One canonical valid request. Every HTTP test varies from this rather than
# restating it, so a schema change lands in exactly one place.
VALID_ORDER = {"customer_email": "a@b.com", "item_sku": "SKU-1", "quantity": 2}
```

`tests/integration/test_healthz.py`:

```python
from conftest import RunAsync, SessionFactory, api_client


def test_healthz_reports_ok_when_postgres_is_reachable(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, str]]:
        async with api_client(factory) as client:
            response = await client.get("/healthz")
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 200
    assert body == {"status": "ok"}
```

- [x] **Step 2: Run the test and watch it fail**

Run: `uv run pytest tests/integration/test_healthz.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.main'`

- [x] **Step 3: Add the two settings**

In `src/outboxexpress/shared/config.py`, alongside `database_url`:

```python
    api_host: str = "127.0.0.1"
    api_port: int = 8000
```

- [x] **Step 4: Write `dependencies.py`**

```python
"""Request-scoped wiring. Everything the routes need arrives through these."""

from collections.abc import AsyncIterator
from typing import Annotated, cast

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def get_sessionmaker(request: Request) -> async_sessionmaker[AsyncSession]:
    # ``app.state`` is dynamically typed. The lifespan is its only writer, so the
    # cast states an invariant rather than papering over an unknown.
    return cast(async_sessionmaker[AsyncSession], request.app.state.sessionmaker)


async def get_session(
    factory: Annotated[async_sessionmaker[AsyncSession], Depends(get_sessionmaker)],
) -> AsyncIterator[AsyncSession]:
    # No ``begin()`` here. ``place_order`` opens and owns the transaction, and
    # starting one now would make it raise "a transaction is already begun".
    async with factory() as session:
        yield session


# `SessionDep` follows FastAPI's own naming for a shared Annotated dependency, and
# avoids a bare `Session` colliding with SQLAlchemy's class of that name.
SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKeyDep = Annotated[str, Header(alias="Idempotency-Key", min_length=1)]
```

- [x] **Step 5: Write `routes.py` with `/healthz` only**

```python
"""The HTTP surface. Routes translate; they do not decide."""

from fastapi import APIRouter
from sqlalchemy import text

from .dependencies import SessionDep

router = APIRouter()


@router.get("/healthz")
async def healthz(session: SessionDep) -> dict[str, str]:
    # Postgres only. The API is healthy without a broker -- that is the pattern
    # working as intended, not a gap in the check (spec 11).
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}
```

- [x] **Step 6: Write `main.py`**

```python
"""Application assembly and the ``outbox-api`` entrypoint."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from outboxexpress.shared.config import get_settings
from outboxexpress.shared.db import create_async_engine_from_settings, create_sessionmaker
from outboxexpress.shared.logging import configure_logging

from .routes import router


# AsyncGenerator, not AsyncIterator: typeshed deprecates the AsyncIterator
# return annotation under @asynccontextmanager. Plain dependency generators
# such as `get_session` keep AsyncIterator.
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # Process startup, not app construction: `create_app` stays free of side
    # effects, so building an app in a test does not reconfigure global logging.
    configure_logging()
    # One engine per process, built after the event loop exists: an async pool
    # binds to the loop that created it.
    engine = create_async_engine_from_settings()
    app.state.sessionmaker = create_sessionmaker(engine)
    try:
        yield
    finally:
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Flash Order Notifier", version="0.1.0", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()


def run() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
```

- [x] **Step 7: Run the test and watch it pass**

Run: `uv run pytest tests/integration/test_healthz.py -v`
Expected: PASS

- [x] **Step 8: Gate**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest -q`
Expected: all clean

---

### Task 2: `POST /orders` returns `201` with the stored body

**Files:**
- Modify: `src/outboxexpress/api/routes.py`
- Test: `tests/integration/test_post_orders.py`

**Interfaces:**
- Consumes: `place_order`, `StoredResponse` (from `api/service.py`); `NewOrder`, `OrderResponse` (from `api/schemas.py`); `SessionDep`, `IdempotencyKeyDep` (from `api/dependencies.py`)
- Produces: `POST /orders`

- [x] **Step 1: Write the failing test**

```python
from typing import Any

from sqlalchemy import Engine, text

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client


def test_post_orders_creates_an_order(run_async: RunAsync, sync_engine: Engine) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any]]:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders", json=VALID_ORDER, headers={"Idempotency-Key": "k-1"}
            )
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 201
    assert body["status"] == "CREATED"
    assert body["order_id"]

    with sync_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM orders")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM outbox")).scalar_one() == 1
```

- [x] **Step 2: Run the test and watch it fail**

Run: `uv run pytest tests/integration/test_post_orders.py -v`
Expected: FAIL — `assert 404 == 201`

- [x] **Step 3: Add the route**

Replace the import block at the top of `src/outboxexpress/api/routes.py` with:

```python
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from .dependencies import IdempotencyKeyDep, SessionDep
from .schemas import NewOrder, OrderResponse
from .service import place_order
```

and append the route:

```python
@router.post(
    "/orders",
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"model": OrderResponse},
        400: {"description": "Validation error or missing Idempotency-Key"},
        409: {"description": "Idempotency-Key reused with a different body"},
        503: {"description": "Database unavailable"},
    },
)
async def create_order(
    session: SessionDep, idempotency_key: IdempotencyKeyDep, order: NewOrder
) -> JSONResponse:
    stored = await place_order(session, idempotency_key=idempotency_key, request=order)
    # A raw JSONResponse, deliberately, not a response_model: a replay must hand back
    # the bytes that were stored, and a response_model would re-serialise them from a
    # freshly built object (spec 11).
    return JSONResponse(status_code=stored.status_code, content=stored.body)
```

- [x] **Step 4: Run the test and watch it pass**

Run: `uv run pytest tests/integration/test_post_orders.py -v`
Expected: PASS

- [x] **Step 5: Gate**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest -q`

---

### Task 3: Every documented failure maps to its documented status

**Files:**
- Create: `src/outboxexpress/api/errors.py`
- Modify: `src/outboxexpress/api/main.py` (call `register_error_handlers`)
- Test: `tests/integration/test_error_contract.py`

**Interfaces:**
- Consumes: `IdempotencyConflict` (from `api/idempotency.py`)
- Produces: `register_error_handlers(app: FastAPI) -> None`

- [x] **Step 1: Write the failing test**

```python
from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client


def test_missing_idempotency_key_is_400(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            return (await client.post("/orders", json=VALID_ORDER)).status_code

    assert run_async(scenario) == 400


def test_invalid_body_is_400(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders",
                json={**VALID_ORDER, "quantity": 0},
                headers={"Idempotency-Key": "k-1"},
            )
            return response.status_code

    assert run_async(scenario) == 400


def test_reused_key_with_a_different_body_is_409(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            headers = {"Idempotency-Key": "k-1"}
            await client.post("/orders", json=VALID_ORDER, headers=headers)
            response = await client.post(
                "/orders", json={**VALID_ORDER, "quantity": 9}, headers=headers
            )
            return response.status_code

    assert run_async(scenario) == 409
```

- [x] **Step 2: Run the tests and watch them fail**

Run: `uv run pytest tests/integration/test_error_contract.py -v`
Expected: two FAIL with `422 != 400`, one FAIL with a 500 from the unhandled `IdempotencyConflict`

- [x] **Step 3: Write `errors.py`**

Handlers are module-level and registered by name rather than with
`@app.exception_handler`: pyright strict reports a decorated nested function as
unused, and naming them keeps each one testable on its own.

```python
"""Failures are handlers, not branches. Routes stay free of status codes."""

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from outboxexpress.shared.logging import get_logger

from .idempotency import IdempotencyConflict

log = get_logger(__name__)

# Every handler takes `exc: Exception` because that is Starlette's registered
# handler signature; each narrows with an assert where it needs the real type.
# JSONResponse's first positional parameter is `content`, not `status_code`.


def register_error_handlers(app: FastAPI) -> None:
    """Map each failure the spec names to its status code, once, in one place."""
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(IdempotencyConflict, handle_idempotency_conflict)
    app.add_exception_handler(OperationalError, handle_database_unavailable)


async def handle_validation_error(_: Request, exc: Exception) -> JSONResponse:
    """400, not FastAPI's default 422.

    The spec names 400 for both a malformed body and a missing Idempotency-Key,
    and FastAPI reports a missing required header as a validation error.
    """
    assert isinstance(exc, RequestValidationError)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": jsonable_encoder(exc.errors())},
    )


async def handle_idempotency_conflict(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, IdempotencyConflict)
    log.info("idempotency_conflict", idempotency_key=exc.key)
    return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})


async def handle_database_unavailable(_: Request, exc: Exception) -> JSONResponse:
    """Postgres is unreachable.

    Say so honestly rather than returning a 500, which a client would read as
    "your request was malformed" and stop retrying.
    """
    log.error("database_unavailable", error=str(exc))
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "database unavailable"},
    )
```

- [x] **Step 4: Register the handlers**

In `create_app`, after `app.include_router(router)`:

```python
    register_error_handlers(app)
```

and import it: `from .errors import register_error_handlers`

- [x] **Step 5: Run the tests and watch them pass**

Run: `uv run pytest tests/integration/test_error_contract.py -v`
Expected: 3 PASS

- [x] **Step 6: Gate**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest -q`

---

### Task 4: A replay over HTTP is byte-identical

**Files:**
- Test: `tests/invariants/test_i7_http_replay.py`

**Interfaces:**
- Consumes: everything from tasks 1–3. Produces no source changes — this task proves the contract or fails.

- [x] **Step 1: Write the failing test**

```python
from httpx import Response
from sqlalchemy import Engine, text

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client


def test_replay_returns_the_stored_bytes_unchanged(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    """Invariant I7 at the HTTP edge: a retrying client cannot tell a replay
    from the original, and never learns its first attempt had already succeeded."""

    async def scenario(factory: SessionFactory) -> tuple[Response, Response]:
        async with api_client(factory) as client:
            headers = {"Idempotency-Key": "k-1"}
            first = await client.post("/orders", json=VALID_ORDER, headers=headers)
            second = await client.post("/orders", json=VALID_ORDER, headers=headers)
            return first, second

    first, second = run_async(scenario)

    assert first.status_code == second.status_code == 201
    assert first.content == second.content

    with sync_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM orders")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM outbox")).scalar_one() == 1
```

- [x] **Step 2: Run it**

Run: `uv run pytest tests/invariants/test_i7_http_replay.py -v`
Expected: PASS. If the bodies differ, the route is rebuilding the response instead of
returning the stored one — fix the route, not the test.

- [x] **Step 3: Manual smoke check**

```bash
uv run outbox-api &
API_PID=$!
for _ in $(seq 20); do curl -sf localhost:8000/healthz >/dev/null && break; sleep 0.5; done
for _ in 1 2; do
  curl -si -XPOST localhost:8000/orders \
    -H 'Idempotency-Key: smoke-1' -H 'content-type: application/json' \
    -d '{"customer_email":"a@b.com","item_sku":"SKU-1","quantity":2}'
done
kill "$API_PID"
```

Expected: `201`, and an identical body on a second identical call. Requires a Postgres
reachable at `OUTBOX_DATABASE_URL`; skip until plan 5 if there is none.

- [x] **Step 4: Gate**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest -q`

---

## Definition of done

- [x] `uv run pytest -q` green
- [x] `uv run ruff check . && uv run ruff format --check .` clean
- [x] `uv run pyright` reports 0 errors
- [x] Nothing under `src/` imports a Kafka client
- [x] `POST /orders` returns 201 / 400 / 409 / 503 exactly as spec §11 states
- [x] A replay is byte-identical to the original response

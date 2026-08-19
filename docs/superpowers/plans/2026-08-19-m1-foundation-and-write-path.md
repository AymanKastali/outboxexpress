# M1 — Foundation and Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commit an order and its `OrderCreated` outbox row in one Postgres transaction, behind an idempotent `POST /orders`, with no message broker anywhere in the project.

**Architecture:** A single `uv` package, `src/outboxexpress/`, with a `shared` layer (settings, logging, engine/session factories, ORM models, event contract) and one service, `api` (FastAPI, async SQLAlchemy). The write path is one function that opens one transaction and inserts three rows. Idempotency is enforced by the `idempotency_keys` primary key, so concurrency is resolved by Postgres rather than by application locking.

**Tech Stack:** Python 3.14 · FastAPI · SQLAlchemy 2.0 (async) · `psycopg[binary]` 3 · PostgreSQL 18 · Alembic · pydantic-settings · structlog · pytest + testcontainers · uv · ruff

**Spec:** `docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md`

## Global Constraints

Copied verbatim from the spec. Every task's requirements implicitly include this section.

- **Python 3.14.** Container base image `python:3.14-slim`.
- **PostgreSQL 18.** Container image `postgres:18`.
- **Exact pinned versions** (spec §3.6), declared with `==` in `pyproject.toml`:
  `fastapi==0.141.1` · `uvicorn==0.52.3` · `sqlalchemy==2.0.52` · `psycopg[binary]==3.3.4` ·
  `alembic==1.19.1` · `pydantic==2.13.4` · `pydantic-settings==2.15.0` · `structlog==26.1.0` ·
  `pytest==9.1.1` · `testcontainers==4.15.0` · `httpx==0.28.1` · `ruff==0.16.3`
- **`api` has no Kafka client and no Kafka configuration** (spec §4). Nothing in M1 imports `confluent-kafka`, and no Kafka settings appear in `shared/config.py`. If a task seems to need a broker, the task is wrong.
- **No `pytest-asyncio`** (spec §3.6). Async tests call `asyncio.run(...)` inside an ordinary `def test_...` function.
- **Dependency direction is one-way** (spec §4.1): `api` imports `shared`. `shared` imports nothing from `api`.
- **`outbox.id` IS the `event_id`** (spec §5.2, §5.5). One UUID, generated once, never re-generated downstream.
- **An idempotent replay returns the stored `response_code` and `response_body` verbatim** (spec §11), not a freshly built response.
- **Order status is always `'CREATED'`** in this scope (spec §2).

### Two deliberate additions to the spec, made here

1. **`uuid.uuid7()` instead of `uuid4()` for primary keys.** New in the Python 3.14 stdlib (RFC 9562). Time-ordered UUIDs insert at the right edge of the B-tree instead of scattering across it, which matters for a table the relay scans by time. The spec says `uuid`; v7 is still a `uuid` column. Zero cost.
2. **`pydantic[email]`** added to dependencies so `EmailStr` works. FastAPI's documented way to validate an email field. `email-validator` is pinned by `uv.lock`, not by hand.

### One file the spec's §10 layout does not list

`src/outboxexpress/api/dependencies.py`. The session dependency cannot live in `main.py` (routes would import main, main imports routes — a cycle) and does not belong in `routes.py`. Nine lines, one responsibility.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `pyproject.toml` | Package metadata, pinned dependencies, ruff + pytest config |
| `src/outboxexpress/shared/config.py` | `Settings` (pydantic-settings) and the cached `get_settings()` |
| `src/outboxexpress/shared/logging.py` | structlog configuration; `event_id` correlation via contextvars |
| `src/outboxexpress/shared/db.py` | Engine and sessionmaker **factories**. Owns no global state. |
| `src/outboxexpress/shared/models.py` | `Base`, naming convention, the four tables, the two partial indexes |
| `src/outboxexpress/shared/events.py` | `OrderCreatedV1` — the wire contract |
| `migrations/env.py`, `migrations/versions/0001_*.py` | Alembic; the initial schema |
| `src/outboxexpress/api/schemas.py` | `NewOrder` request, `OrderResponse` response |
| `src/outboxexpress/api/idempotency.py` | Request fingerprint, key lookup, `IdempotencyConflict` |
| `src/outboxexpress/api/service.py` | **The one transaction.** The only place that writes orders + outbox. |
| `src/outboxexpress/api/dependencies.py` | `SessionDep` |
| `src/outboxexpress/api/routes.py` | `POST /orders`, `GET /orders/{id}`, `GET /healthz` |
| `src/outboxexpress/api/main.py` | App, lifespan-managed engine |
| `tests/conftest.py` | testcontainers Postgres, migrations, truncation, `run_async` |
| `tests/unit/`, `tests/invariants/` | Unit tests; I2 and I7 |
| `Dockerfile`, `docker-compose.yml`, `Makefile` | Run it for real |

---

## Task 1: Project skeleton and settings

**Files:**
- Create: `pyproject.toml`, `.python-version`, `src/outboxexpress/__init__.py`, `src/outboxexpress/shared/__init__.py`, `src/outboxexpress/shared/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: nothing
- Produces: `outboxexpress.shared.config.Settings` with fields `database_url: str`, `log_level: str`, `log_json: bool`; `get_settings() -> Settings` (LRU-cached, exposes `.cache_clear()`)

- [ ] **Step 1: Create the package skeleton**

```bash
mkdir -p src/outboxexpress/shared src/outboxexpress/api tests/unit tests/invariants
touch src/outboxexpress/__init__.py src/outboxexpress/shared/__init__.py src/outboxexpress/api/__init__.py
echo "3.14" > .python-version
```

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "outboxexpress"
version = "0.1.0"
description = "Flash Order Notifier — a transactional outbox study"
requires-python = "==3.14.*"
dependencies = [
    "fastapi==0.141.1",
    "uvicorn==0.52.3",
    "sqlalchemy==2.0.52",
    "psycopg[binary]==3.3.4",
    "alembic==1.19.1",
    "pydantic[email]==2.13.4",
    "pydantic-settings==2.15.0",
    "structlog==26.1.0",
]

[dependency-groups]
dev = [
    "pytest==9.1.1",
    "testcontainers==4.15.0",
    "httpx==0.28.1",
    "ruff==0.16.3",
]

[project.scripts]
outbox-api = "outboxexpress.api.main:run"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/outboxexpress"]

[tool.ruff]
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "RUF"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q --strict-markers"
```

- [ ] **Step 3: Write the failing test**

Create `tests/unit/test_config.py`:

```python
from outboxexpress.shared.config import Settings, get_settings


def test_settings_read_the_outbox_prefixed_environment(monkeypatch):
    monkeypatch.setenv("OUTBOX_DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    monkeypatch.setenv("OUTBOX_LOG_LEVEL", "DEBUG")
    settings = Settings()
    assert settings.database_url == "postgresql+psycopg://u:p@db:5432/x"
    assert settings.log_level == "DEBUG"


def test_get_settings_is_cached():
    assert get_settings() is get_settings()
```

- [ ] **Step 4: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.shared.config'`

- [ ] **Step 5: Write `src/outboxexpress/shared/config.py`**

```python
"""Typed, environment-driven settings shared by every service."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OUTBOX_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # No Kafka settings here. The api is the only M1 service and it has no broker.
    database_url: str = "postgresql+psycopg://outbox:outbox@localhost:5432/outbox"
    log_level: str = "INFO"
    log_json: bool = True


@lru_cache
def get_settings() -> Settings:
    """Process-wide settings. Call ``get_settings.cache_clear()`` in tests."""
    return Settings()
```

- [ ] **Step 6: Run it and watch it pass**

Run: `uv sync && uv run pytest tests/unit/test_config.py -v`
Expected: 2 passed

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add pyproject.toml uv.lock .python-version src tests
git commit -m "feat: package skeleton and typed settings"
```

---

## Task 2: Structured logging

**Files:**
- Create: `src/outboxexpress/shared/logging.py`
- Test: `tests/unit/test_logging.py`

**Interfaces:**
- Consumes: `get_settings()`
- Produces: `configure_logging() -> None`; `get_logger(name: str) -> structlog.BoundLogger`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_logging.py`:

```python
import json

import structlog

from outboxexpress.shared.logging import configure_logging, get_logger


def test_bound_event_id_appears_in_every_json_line(capsys):
    configure_logging()
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(event_id="0199a0f0-0000-7000-8000-000000000000")
    try:
        get_logger("test").info("order_committed", order_id="abc")
    finally:
        structlog.contextvars.clear_contextvars()

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["event"] == "order_committed"
    assert payload["order_id"] == "abc"
    assert payload["event_id"] == "0199a0f0-0000-7000-8000-000000000000"
    assert payload["level"] == "info"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.shared.logging'`

- [ ] **Step 3: Write `src/outboxexpress/shared/logging.py`**

`merge_contextvars` is what makes `event_id` correlation work: bind it once at the top
of a request or a relay cycle and every line beneath carries it.

```python
"""structlog configuration. ``event_id`` is the correlation key across all services."""

import logging
from typing import Any

import structlog

from .config import get_settings


def configure_logging() -> None:
    settings = get_settings()
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if settings.log_json
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log_level.upper()]
        ),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/shared/logging.py tests/unit/test_logging.py
git commit -m "feat: structlog configuration with event_id correlation"
```

---

## Task 3: ORM models and the two partial indexes

**Files:**
- Create: `src/outboxexpress/shared/models.py`
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Base`, `OutboxStatus` (StrEnum: `PENDING`/`PUBLISHING`/`PUBLISHED`/`DEAD`), `Order`, `Outbox`, `IdempotencyKey`, `ProcessedEvent`, `new_id() -> uuid.UUID`, `utcnow() -> datetime`, `ORDER_STATUS_CREATED`, `AGGREGATE_TYPE_ORDER`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_models.py`:

```python
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from outboxexpress.shared.models import Base, OutboxStatus, new_id


def test_the_four_tables_are_registered():
    assert set(Base.metadata.tables) == {
        "orders",
        "outbox",
        "idempotency_keys",
        "processed_events",
    }


def test_outbox_indexes_are_partial():
    indexes = {ix.name: ix for ix in Base.metadata.tables["outbox"].indexes}
    pending = str(CreateIndex(indexes["ix_outbox_pending"]).compile(dialect=postgresql.dialect()))
    reclaim = str(CreateIndex(indexes["ix_outbox_reclaim"]).compile(dialect=postgresql.dialect()))
    assert "next_attempt_at" in pending and "status = 'pending'" in pending
    assert "locked_until" in reclaim and "status = 'publishing'" in reclaim


def test_outbox_status_serialises_as_lowercase_values():
    assert OutboxStatus.PENDING == "pending"
    assert [s.value for s in OutboxStatus] == ["pending", "publishing", "published", "dead"]


def test_new_id_returns_time_ordered_uuid7():
    first, second = new_id(), new_id()
    assert first.version == 7
    assert first < second  # v7 sorts by creation time
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.shared.models'`

- [ ] **Step 3: Write `src/outboxexpress/shared/models.py`**

Three details are load-bearing and easy to get wrong:

- `values_callable` on the `Enum`. Without it SQLAlchemy writes the enum **member names**
  (`PENDING`) into Postgres, not the values (`pending`), and the partial-index predicates
  in the spec stop matching anything.
- `postgresql_where` is what makes the indexes partial. Spec §5.2 calls this load-bearing:
  without it, the relay's claim index grows alongside millions of published rows.
- Timestamps carry a Python-side `default` **and** a `server_default`. The Python default
  means the value is known without a round trip, so the API can build its response body
  before commit; the server default is the safety net for anything inserted by hand.

```python
"""The four tables. Every guarantee here is a constraint, not application code being careful."""

import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

ORDER_STATUS_CREATED = "CREATED"
AGGREGATE_TYPE_ORDER = "order"

# Deterministic constraint names, so Alembic autogenerate can always find them again.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> uuid.UUID:
    """UUIDv7 — random, but time-ordered, so primary keys append to the B-tree."""
    return uuid.uuid7()


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class OutboxStatus(enum.StrEnum):
    # StrEnum's auto() is the lowercased member name — the values the partial
    # index predicates in the migration match on.
    PENDING = enum.auto()
    PUBLISHING = enum.auto()
    PUBLISHED = enum.auto()
    DEAD = enum.auto()


_outbox_status = Enum(
    OutboxStatus,
    name="outbox_status",
    values_callable=lambda e: [member.value for member in e],
)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    customer_email: Mapped[str] = mapped_column(Text, nullable=False)
    item_sku: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default=ORDER_STATUS_CREATED)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )

    __table_args__ = (CheckConstraint("quantity > 0", name="quantity_positive"),)


class Outbox(Base):
    """The event above the line; relay bookkeeping below it."""

    __tablename__ = "outbox"

    # This id IS the event_id, carried unchanged to Kafka and on to the consumer.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )

    # --- relay state, private to the relay (M2) ---
    status: Mapped[OutboxStatus] = mapped_column(
        _outbox_status, nullable=False, default=OutboxStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )
    locked_by: Mapped[str | None] = mapped_column(Text)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_outbox_reclaim",
            "locked_until",
            postgresql_where=text("status = 'publishing'"),
        ),
    )


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"), nullable=False)
    response_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )


class ProcessedEvent(Base):
    """Consumer-side dedup (M3). Composite key so a second handler dedups independently."""

    __tablename__ = "processed_events"

    handler: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/shared/models.py tests/unit/test_models.py
git commit -m "feat: ORM models with partial outbox indexes"
```

---

## Task 4: Engine factories and the Alembic migration

**Files:**
- Create: `src/outboxexpress/shared/db.py`, `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `migrations/versions/0001_initial_schema.py`
- Test: `tests/conftest.py`, `tests/unit/test_migrations.py`

**Interfaces:**
- Consumes: `get_settings()`, `Base`
- Produces: `create_async_engine_from_settings() -> AsyncEngine`; `create_sessionmaker(engine) -> async_sessionmaker[AsyncSession]`; `create_sync_engine_from_settings() -> Engine`

- [ ] **Step 1: Write `src/outboxexpress/shared/db.py`**

These are **factories**, not module-level singletons. An async engine binds its connection
pool to the event loop that created it, so a cached global blows up the moment a second
loop appears — which is exactly what the concurrency test in Task 9 does.

```python
"""Engine and session factories. Deliberately no module-level engine."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import get_settings


def create_async_engine_from_settings() -> AsyncEngine:
    """psycopg3 serves both sync and async behind one URL — see spec 3.5."""
    return create_async_engine(get_settings().database_url, pool_pre_ping=True)


def create_sync_engine_from_settings() -> Engine:
    """Used by Alembic and by tests that need to observe committed state."""
    return create_engine(get_settings().database_url, pool_pre_ping=True)


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: the write path reads attributes off an instance *after*
    # commit to build its response. With expiry on, that would trigger lazy IO.
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 2: Initialise Alembic and point it at the models**

```bash
uv run alembic init -t generic migrations
```

Then replace the generated `migrations/env.py` with:

```python
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool

from outboxexpress.shared.config import get_settings
from outboxexpress.shared.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", get_settings().database_url)
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    from sqlalchemy import engine_from_config

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

In `alembic.ini`, set `script_location = migrations` and leave `sqlalchemy.url` empty —
`env.py` overrides it from settings, so the database URL has exactly one source.

- [ ] **Step 3: Write the initial migration by hand**

Create `migrations/versions/0001_initial_schema.py`. Written out rather than autogenerated:
autogenerate creates the enum type as a side effect of the column and then **fails to drop
it on downgrade**, leaving `outbox_status` orphaned so the next upgrade errors with
"type already exists". Creating and dropping it explicitly makes downgrade actually work.

```python
"""initial schema

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

outbox_status = postgresql.ENUM(
    "pending", "publishing", "published", "dead", name="outbox_status", create_type=False
)


def upgrade() -> None:
    outbox_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "orders",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("customer_email", sa.Text(), nullable=False),
        sa.Column("item_sku", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("quantity > 0", name="ck_orders_quantity_positive"),
        sa.PrimaryKeyConstraint("id", name="pk_orders"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("status", outbox_status, nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["next_attempt_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_outbox_reclaim",
        "outbox",
        ["locked_until"],
        postgresql_where=sa.text("status = 'publishing'"),
    )

    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("response_code", sa.Integer(), nullable=False),
        sa.Column("response_body", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["orders.id"], name="fk_idempotency_keys_order_id_orders"
        ),
        sa.PrimaryKeyConstraint("key", name="pk_idempotency_keys"),
    )

    op.create_table(
        "processed_events",
        sa.Column("handler", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("handler", "event_id", name="pk_processed_events"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_table("idempotency_keys")
    op.drop_index("ix_outbox_reclaim", table_name="outbox")
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("orders")
    outbox_status.drop(op.get_bind(), checkfirst=True)
```

- [ ] **Step 4: Write `tests/conftest.py`**

Note the two decisions worth understanding:

- Tables are **truncated** between tests rather than wrapped in a rolled-back transaction.
  The idempotency test in Task 9 needs genuinely concurrent committed transactions, and a
  wrapping transaction would make them invisible to each other.
- `run_async` builds the engine **inside** the loop it runs, for the reason given in Task 4
  Step 1.

```python
import asyncio
import os
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.postgres import PostgresContainer

from outboxexpress.shared.config import get_settings
from outboxexpress.shared.db import (
    create_async_engine_from_settings,
    create_sessionmaker,
    create_sync_engine_from_settings,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLES = "orders, outbox, idempotency_keys, processed_events"


@pytest.fixture(scope="session", autouse=True)
def database() -> Iterator[str]:
    with PostgresContainer("postgres:18", driver="psycopg") as container:
        os.environ["OUTBOX_DATABASE_URL"] = container.get_connection_url()
        get_settings.cache_clear()
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        command.upgrade(config, "head")
        yield container.get_connection_url()


@pytest.fixture(scope="session")
def sync_engine(database: str) -> Iterator[Engine]:
    engine = create_sync_engine_from_settings()
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(sync_engine: Engine) -> Iterator[None]:
    yield
    with sync_engine.begin() as connection:
        connection.execute(text(f"TRUNCATE {TABLES} CASCADE"))


@pytest.fixture
def run_async() -> Callable[[Callable[[async_sessionmaker[AsyncSession]], Awaitable[Any]]], Any]:
    """Run a coroutine that needs sessions, building the engine inside the loop."""

    def _run(scenario: Callable[[async_sessionmaker[AsyncSession]], Awaitable[Any]]) -> Any:
        async def main() -> Any:
            engine = create_async_engine_from_settings()
            try:
                return await scenario(create_sessionmaker(engine))
            finally:
                await engine.dispose()

        return asyncio.run(main())

    return _run
```

- [ ] **Step 5: Write the failing migration test**

Create `tests/unit/test_migrations.py`:

```python
from sqlalchemy import Engine, inspect, text


def test_migration_creates_the_four_tables(sync_engine: Engine):
    tables = set(inspect(sync_engine).get_table_names())
    assert {"orders", "outbox", "idempotency_keys", "processed_events"} <= tables


def test_outbox_partial_indexes_exist_in_postgres(sync_engine: Engine):
    with sync_engine.connect() as connection:
        rows = dict(
            connection.execute(
                text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'outbox'")
            ).all()
        )
    assert "WHERE (status = 'pending'" in rows["ix_outbox_pending"]
    assert "WHERE (status = 'publishing'" in rows["ix_outbox_reclaim"]


def test_downgrade_then_upgrade_is_clean(sync_engine: Engine):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    assert "outbox" in inspect(sync_engine).get_table_names()
```

- [ ] **Step 6: Run it**

Run: `uv run pytest tests/unit/test_migrations.py -v`
Expected: 3 passed. If `test_downgrade_then_upgrade_is_clean` fails with
`type "outbox_status" already exists`, the enum is not being dropped — revisit Step 3's `downgrade`.

- [ ] **Step 7: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/shared/db.py alembic.ini migrations tests
git commit -m "feat: engine factories and initial Alembic migration"
```

---

## Task 5: The event contract

**Files:**
- Create: `src/outboxexpress/shared/events.py`
- Test: `tests/unit/test_events.py`

**Interfaces:**
- Consumes: nothing
- Produces: `OrderSnapshot`, `OrderCreatedV1` with fields `event_id: UUID`, `event_type: Literal["OrderCreated"]`, `version: Literal[1]`, `occurred_at: datetime`, `order: OrderSnapshot`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_events.py`:

```python
from datetime import UTC, datetime
from uuid import UUID

from outboxexpress.shared.events import OrderCreatedV1, OrderSnapshot


def test_json_shape_matches_the_wire_contract():
    event = OrderCreatedV1(
        event_id=UUID("0199a0f0-0000-7000-8000-000000000001"),
        occurred_at=datetime(2026, 8, 19, 7, 0, tzinfo=UTC),
        order=OrderSnapshot(
            id=UUID("0199a0f0-0000-7000-8000-000000000002"),
            customer_email="a@b.com",
            item_sku="SKU-1",
            quantity=2,
            created_at=datetime(2026, 8, 19, 7, 0, tzinfo=UTC),
        ),
    )
    payload = event.model_dump(mode="json")
    assert payload["event_type"] == "OrderCreated"
    assert payload["version"] == 1
    assert set(payload) == {"event_id", "event_type", "version", "occurred_at", "order"}
    assert set(payload["order"]) == {"id", "customer_email", "item_sku", "quantity", "created_at"}
    # JSON round-trip is what the relay and the notifier both depend on.
    assert OrderCreatedV1.model_validate(payload) == event
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_events.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.shared.events'`

- [ ] **Step 3: Write `src/outboxexpress/shared/events.py`**

```python
"""The wire contract. Imported by the writer and, from M2 on, by every reader."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OrderSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    customer_email: str
    item_sku: str
    quantity: int
    created_at: datetime


class OrderCreatedV1(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID
    event_type: Literal["OrderCreated"] = "OrderCreated"
    version: Literal[1] = 1
    occurred_at: datetime
    order: OrderSnapshot
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_events.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/shared/events.py tests/unit/test_events.py
git commit -m "feat: OrderCreatedV1 wire contract"
```

---

## Task 6: Request schemas and the idempotency fingerprint

**Files:**
- Create: `src/outboxexpress/api/schemas.py`, `src/outboxexpress/api/idempotency.py`
- Test: `tests/unit/test_idempotency.py`

**Interfaces:**
- Consumes: `IdempotencyKey` model
- Produces: `NewOrder(customer_email: EmailStr, item_sku: str, quantity: int)`; `OrderResponse`; `request_fingerprint(request: NewOrder) -> str`; `load_key(session, key) -> IdempotencyKey | None`; `IdempotencyConflict(Exception)`

- [ ] **Step 1: Write `src/outboxexpress/api/schemas.py`**

```python
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class NewOrder(BaseModel):
    # forbid: an unrecognised field must be a client bug, not a silently dropped one.
    model_config = ConfigDict(extra="forbid", frozen=True)

    customer_email: EmailStr
    item_sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0)


class OrderResponse(BaseModel):
    order_id: UUID
    status: str
    created_at: datetime
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/test_idempotency.py`:

```python
import pytest
from pydantic import ValidationError

from outboxexpress.api.idempotency import request_fingerprint
from outboxexpress.api.schemas import NewOrder


def order(**overrides) -> NewOrder:
    return NewOrder(
        **{"customer_email": "a@b.com", "item_sku": "SKU-1", "quantity": 2, **overrides}
    )


def test_fingerprint_is_stable_for_equal_requests():
    assert request_fingerprint(order()) == request_fingerprint(order())


def test_fingerprint_changes_when_any_field_changes():
    baseline = request_fingerprint(order())
    assert request_fingerprint(order(quantity=3)) != baseline
    assert request_fingerprint(order(item_sku="SKU-2")) != baseline


def test_fingerprint_is_a_sha256_hex_digest():
    assert len(request_fingerprint(order())) == 64


def test_invalid_requests_never_reach_the_database():
    with pytest.raises(ValidationError):
        order(quantity=0)
    with pytest.raises(ValidationError):
        order(customer_email="not-an-email")
```

- [ ] **Step 3: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_idempotency.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.idempotency'`

- [ ] **Step 4: Write `src/outboxexpress/api/idempotency.py`**

Hashing the **validated** model, canonically serialised, is what makes the comparison
mean "same request" rather than "same bytes" — key order and whitespace stop mattering.

```python
"""Idempotency-Key handling, Stripe-style: same key + different body is a client bug."""

import hashlib
import json

from sqlalchemy.ext.asyncio import AsyncSession

from outboxexpress.shared.models import IdempotencyKey

from .schemas import NewOrder


class IdempotencyConflict(Exception):
    """Same Idempotency-Key, different request body."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Idempotency-Key {key!r} was reused with a different body")
        self.key = key


def request_fingerprint(request: NewOrder) -> str:
    canonical = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def load_key(session: AsyncSession, key: str) -> IdempotencyKey | None:
    return await session.get(IdempotencyKey, key)
```

- [ ] **Step 5: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_idempotency.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api/schemas.py src/outboxexpress/api/idempotency.py tests/unit/test_idempotency.py
git commit -m "feat: order request schema and idempotency fingerprint"
```

---

## Task 7: The one transaction

The core of the milestone. Everything else is plumbing around this file.

**Files:**
- Create: `src/outboxexpress/api/service.py`
- Test: `tests/unit/test_service.py`

**Interfaces:**
- Consumes: `NewOrder`, `request_fingerprint`, `load_key`, `IdempotencyConflict`, `Order`, `Outbox`, `IdempotencyKey`, `OrderCreatedV1`, `OrderSnapshot`, `new_id`, `utcnow`
- Produces: `StoredResponse(status_code: int, body: dict)`; `async place_order(session: AsyncSession, *, idempotency_key: str, request: NewOrder) -> StoredResponse`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_service.py`:

```python
import pytest
from sqlalchemy import Engine, text

from outboxexpress.api.idempotency import IdempotencyConflict
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

Run: `uv run pytest tests/unit/test_service.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.service'`

- [ ] **Step 3: Write `src/outboxexpress/api/service.py`**

```python
"""The write path. One transaction, three rows, no network call."""

from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
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

from .idempotency import IdempotencyConflict, load_key, request_fingerprint
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
    """Create an order and its event atomically, or replay a previous identical request."""
    seen = await load_key(session, idempotency_key)
    if seen is not None:
        return _replay(seen, request)

    try:
        async with session.begin():
            record = _write(session, idempotency_key, request)
    except IntegrityError:
        # Another request committed this key first. It blocked us on the primary key
        # until it committed, so by the time we get here the winner is durably visible.
        winner = await load_key(session, idempotency_key)
        if winner is None:
            raise
        log.info("idempotent_race_lost", idempotency_key=idempotency_key)
        return _replay(winner, request)

    # Readable after commit only because the sessionmaker sets expire_on_commit=False.
    log.info("order_committed", order_id=str(record.order_id))
    return StoredResponse(record.response_code, record.response_body)


def _write(session: AsyncSession, idempotency_key: str, request: NewOrder) -> IdempotencyKey:
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
    # Bind the correlation key the relay and notifier will carry (spec 5.5).
    log.info("order_written", order_id=str(order.id), event_id=str(event.event_id))
    # add_all, not three flushes: the unit of work orders the inserts by foreign key.
    session.add_all([order, outbox, record])
    return record


def _replay(record: IdempotencyKey, request: NewOrder) -> StoredResponse:
    if record.request_hash != request_fingerprint(request):
        raise IdempotencyConflict(record.key)
    return StoredResponse(record.response_code, record.response_body)
```

- [ ] **Step 4: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_service.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api/service.py tests/unit/test_service.py
git commit -m "feat: atomic order and outbox write path"
```

---

## Task 8: FastAPI application

**Files:**
- Create: `src/outboxexpress/api/dependencies.py`, `src/outboxexpress/api/routes.py`, `src/outboxexpress/api/main.py`
- Test: `tests/unit/test_routes.py`

**Interfaces:**
- Consumes: `place_order`, `StoredResponse`, `IdempotencyConflict`, `Order`, `OrderResponse`, `create_async_engine_from_settings`, `create_sessionmaker`, `configure_logging`
- Produces: `app: FastAPI`; `run() -> None` console entrypoint; `SessionDep`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_routes.py`:

```python
import pytest
from fastapi.testclient import TestClient

from outboxexpress.api.main import app

BODY = {"customer_email": "a@b.com", "item_sku": "SKU-1", "quantity": 2}


@pytest.fixture
def client(database):
    with TestClient(app) as test_client:
        yield test_client


def test_healthz_reports_postgres_only(client):
    assert client.get("/healthz").json() == {"status": "ok", "database": "ok"}


def test_post_orders_creates_and_returns_201(client):
    response = client.post("/orders", json=BODY, headers={"Idempotency-Key": "k-1"})
    assert response.status_code == 201
    assert response.json()["status"] == "CREATED"


def test_missing_idempotency_key_is_rejected(client):
    assert client.post("/orders", json=BODY).status_code == 400


def test_replay_returns_the_identical_response(client):
    headers = {"Idempotency-Key": "k-1"}
    first = client.post("/orders", json=BODY, headers=headers)
    second = client.post("/orders", json=BODY, headers=headers)
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()


def test_same_key_different_body_is_409(client):
    headers = {"Idempotency-Key": "k-1"}
    client.post("/orders", json=BODY, headers=headers)
    conflict = client.post("/orders", json={**BODY, "quantity": 9}, headers=headers)
    assert conflict.status_code == 409


def test_get_order_round_trips_and_404s(client):
    created = client.post("/orders", json=BODY, headers={"Idempotency-Key": "k-1"}).json()
    assert client.get(f"/orders/{created['order_id']}").status_code == 200
    assert client.get("/orders/0199a0f0-0000-7000-8000-0000000000ff").status_code == 404
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/unit/test_routes.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'outboxexpress.api.main'`

- [ ] **Step 3: Write `src/outboxexpress/api/dependencies.py`**

```python
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
```

- [ ] **Step 4: Write `src/outboxexpress/api/routes.py`**

`POST /orders` returns a `JSONResponse` built from the **stored** body rather than a
serialised model, so a replay is byte-identical to the original (spec §11).

```python
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from outboxexpress.shared.models import Order

from .dependencies import SessionDep
from .idempotency import IdempotencyConflict
from .schemas import NewOrder, OrderResponse
from .service import place_order

router = APIRouter()


@router.get("/healthz")
async def healthz(session: SessionDep) -> dict[str, str]:
    """Postgres only. The API is healthy without a broker — that is the pattern working."""
    try:
        await session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail="database unavailable") from None
    return {"status": "ok", "database": "ok"}


@router.post("/orders", status_code=201, response_model=OrderResponse)
async def create_order(
    request: NewOrder,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JSONResponse:
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    try:
        stored = await place_order(session, idempotency_key=idempotency_key, request=request)
    except IdempotencyConflict as conflict:
        raise HTTPException(status_code=409, detail=str(conflict)) from None
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail="database unavailable") from None
    return JSONResponse(status_code=stored.status_code, content=stored.body)


@router.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: UUID, session: SessionDep) -> OrderResponse:
    order = await session.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    return OrderResponse(order_id=order.id, status=order.status, created_at=order.created_at)
```

- [ ] **Step 5: Write `src/outboxexpress/api/main.py`**

The engine is owned by the lifespan, not by a module global — one place creates it, one
place disposes of it, and tests get a fresh one per `TestClient`.

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from outboxexpress.shared.db import create_async_engine_from_settings, create_sessionmaker
from outboxexpress.shared.logging import configure_logging

from .routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    engine = create_async_engine_from_settings()
    app.state.engine = engine
    app.state.sessionmaker = create_sessionmaker(engine)
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(title="Flash Order Notifier", version="0.1.0", lifespan=lifespan)
app.include_router(router)


def run() -> None:
    import uvicorn

    uvicorn.run("outboxexpress.api.main:app", host="0.0.0.0", port=8000)
```

- [ ] **Step 6: Run it and watch it pass**

Run: `uv run pytest tests/unit/test_routes.py -v`
Expected: 6 passed

- [ ] **Step 7: Commit**

```bash
uv run ruff check --fix . && uv run ruff format .
git add src/outboxexpress/api tests/unit/test_routes.py
git commit -m "feat: FastAPI application with idempotent order endpoint"
```

---

## Task 9: Invariants I2 and I7

These two tests are the deliverable of M1. Everything before them exists so they can pass.

**Files:**
- Create: `tests/invariants/test_i2_no_orphan_events.py`, `tests/invariants/test_i7_edge_idempotency.py`

**Interfaces:**
- Consumes: `place_order`, `NewOrder`, `run_async`, `sync_engine`, `Order`, `Outbox`
- Produces: nothing importable

- [ ] **Step 1: Write I2 — no orphan events**

Create `tests/invariants/test_i2_no_orphan_events.py`:

```python
"""I2: no event exists whose order is not committed, and no order lacks its event."""

import pytest
from sqlalchemy import Engine, text

from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order

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
                    from outboxexpress.api.service import _write

                    _write(session, "k-1", REQUEST)
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

- [ ] **Step 2: Run I2**

Run: `uv run pytest tests/invariants/test_i2_no_orphan_events.py -v`
Expected: 2 passed

- [ ] **Step 3: Write I7 — edge idempotency under real concurrency**

Create `tests/invariants/test_i7_edge_idempotency.py`:

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
            # A separate session per caller: this is a real race in Postgres, not a
            # simulated one. The losers block on the idempotency_keys primary key.
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

- [ ] **Step 4: Run I7**

Run: `uv run pytest tests/invariants/test_i7_edge_idempotency.py -v`
Expected: 1 passed. If it fails with a connection-pool timeout, the async engine is being
shared across event loops — check that `run_async` builds it inside `main()`.

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest -v`
Expected: all passed

- [ ] **Step 6: Commit**

```bash
git add tests/invariants
git commit -m "test: prove invariants I2 (no orphan events) and I7 (edge idempotency)"
```

---

## Task 10: Run it for real

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `Makefile`, `README.md`

**Interfaces:**
- Consumes: `outboxexpress.api.main:app`
- Produces: `make up`, `make test`, `make lint`, `make migrate`, `make smoke`

- [ ] **Step 1: Write the `Dockerfile`**

The layer split — dependencies before source — is what keeps a code edit from
reinstalling the world.

```dockerfile
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

COPY alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["uvicorn", "outboxexpress.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

And `.dockerignore`:

```
.git
.venv
.superpowers
docs
tests
__pycache__
*.pyc
```

- [ ] **Step 2: Write `docker-compose.yml`**

Postgres only. No Kafka in M1 — that is the point being made.

```yaml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: outbox
      POSTGRES_PASSWORD: outbox
      POSTGRES_DB: outbox
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U outbox -d outbox"]
      interval: 2s
      timeout: 3s
      retries: 15

  migrate:
    build: .
    command: ["alembic", "upgrade", "head"]
    environment:
      OUTBOX_DATABASE_URL: postgresql+psycopg://outbox:outbox@postgres:5432/outbox
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    environment:
      OUTBOX_DATABASE_URL: postgresql+psycopg://outbox:outbox@postgres:5432/outbox
      OUTBOX_LOG_JSON: "true"
    ports:
      - "8000:8000"
    depends_on:
      migrate:
        condition: service_completed_successfully

volumes:
  pgdata:
```

- [ ] **Step 3: Write the `Makefile`**

```makefile
.PHONY: up down logs test lint migrate smoke

up:
	docker compose up --build -d
	@echo "api on http://localhost:8000/docs"

down:
	docker compose down -v

logs:
	docker compose logs -f api

test:
	uv run pytest -v

lint:
	uv run ruff check . && uv run ruff format --check .

migrate:
	uv run alembic upgrade head

smoke:
	curl -sS localhost:8000/healthz; echo
	curl -sS -i -X POST localhost:8000/orders \
	  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
	  -d '{"customer_email":"a@b.com","item_sku":"SKU-1","quantity":2}'
	@echo "\n--- same key again: identical 201 ---"
	curl -sS -i -X POST localhost:8000/orders \
	  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
	  -d '{"customer_email":"a@b.com","item_sku":"SKU-1","quantity":2}'
```

- [ ] **Step 4: Bring it up and watch the invariant by hand**

```bash
make up
make smoke
docker compose exec postgres psql -U outbox -d outbox \
  -c "SELECT id, aggregate_id, event_type, status FROM outbox;"
```

Expected: two identical `201` responses, exactly one row in `orders`, exactly one row in
`outbox` with `status = 'pending'`. Nothing will ever publish it — the relay arrives in M2.

- [ ] **Step 5: Write `README.md`**

```markdown
# outboxexpress — Flash Order Notifier

A transactional outbox study. See
[the design spec](docs/superpowers/specs/2026-08-19-flash-order-notifier-design.md).

**M1 (this milestone):** an order and its `OrderCreated` event commit in one Postgres
transaction, behind an idempotent `POST /orders`. There is no message broker in the
project yet — deliberately. The API's durability guarantee is one commit, and it does
not depend on Kafka being reachable, or existing.

## Run it

    make up        # postgres + migrations + api
    make smoke     # post the same order twice, get the same 201 twice
    make test      # pytest against a throwaway Postgres (testcontainers)
    make lint

## What is proved here

- **I2 — no orphan events.** A rolled-back transaction leaves neither an order nor an
  event; every committed order has exactly one event.
- **I7 — edge idempotency.** Eight concurrent posts with one `Idempotency-Key` produce
  one order, one event, and eight identical responses.

## Next

M2 adds the three-broker Kafka cluster and the relay that drains the outbox.
```

- [ ] **Step 6: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml Makefile README.md
git commit -m "feat: docker compose stack and developer makefile"
```

---

## Definition of done

- [ ] `make lint` clean
- [ ] `make test` green, including `tests/invariants/`
- [ ] `make up && make smoke` shows two identical `201` responses
- [ ] `psql` shows one `orders` row and one `pending` `outbox` row after `make smoke`
- [ ] No file in `src/` imports `confluent-kafka` or mentions a broker
- [ ] `alembic downgrade base && alembic upgrade head` runs clean twice in a row

## Carried into M2

Not built here, and not to be built here:

- `relay/` — claim, lease, reclaim, publish, mark, error classification, DLQ
- `hooks.py` failure-injection seams
- Kafka settings in `shared/config.py`
- Invariants I1, I3, I4, I5

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
from testcontainers.community.postgres import PostgresContainer

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
        command.upgrade(Config(str(PROJECT_ROOT / "alembic.ini")), "head")
        yield container.get_connection_url()


@pytest.fixture(scope="session")
def sync_engine(database: str) -> Iterator[Engine]:
    engine = create_sync_engine_from_settings()
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(sync_engine: Engine) -> Iterator[None]:
    # Truncate rather than wrap each test in a rolled-back transaction: the idempotency
    # invariant needs genuinely concurrent transactions that can see each other's commits.
    yield
    with sync_engine.begin() as connection:
        connection.execute(text(f"TRUNCATE {TABLES} CASCADE"))


@pytest.fixture
def run_async() -> Callable[[Callable[[async_sessionmaker[AsyncSession]], Awaitable[Any]]], Any]:
    """Run a coroutine that needs sessions, building the engine inside the loop.

    An async engine binds its pool to the loop that created it, so a shared engine
    would fail the moment a second ``asyncio.run`` opened a new one.
    """

    def _run(scenario: Callable[[async_sessionmaker[AsyncSession]], Awaitable[Any]]) -> Any:
        async def main() -> Any:
            engine = create_async_engine_from_settings()
            try:
                return await scenario(create_sessionmaker(engine))
            finally:
                await engine.dispose()

        return asyncio.run(main())

    return _run

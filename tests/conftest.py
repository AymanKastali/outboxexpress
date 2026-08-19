import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator
from concurrent.futures import Future
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config

# NewTopic is the documented import path, but the admin package never re-exports it,
# so pyright strict reads it as private. The library's packaging is what is imprecise.
from confluent_kafka.admin import AdminClient, NewTopic  # pyright: ignore[reportPrivateImportUsage]
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.community.postgres import PostgresContainer

from kafka_container import ApacheKafkaContainer
from outboxexpress.api.dependencies import get_session
from outboxexpress.api.main import create_app
from outboxexpress.shared.config import get_settings
from outboxexpress.shared.db import (
    create_async_engine_from_settings,
    create_sessionmaker,
    create_sync_engine_from_settings,
    create_sync_sessionmaker,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLES = "orders, outbox, idempotency_keys, processed_events"

# One canonical valid request. Every HTTP test varies from this rather than
# restating it, so a schema change lands in exactly one place.
VALID_ORDER = {"customer_email": "a@b.com", "item_sku": "SKU-1", "quantity": 2}

TOPIC = "orders.events"
PARTITIONS = 3

# What create_topics really returns. The library annotates it as a bare ``Future``,
# which pyright strict reads as partially unknown, so the type is restated here once.
TopicFutures = dict[str, Future[None]]

SessionFactory = async_sessionmaker[AsyncSession]


class RunAsync(Protocol):
    """Runs a session-using coroutine and hands back whatever it returned."""

    def __call__[T](self, scenario: Callable[[SessionFactory], Awaitable[T]]) -> T: ...


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


@pytest.fixture
def relay_sessions(sync_engine: Engine) -> sessionmaker[Session]:
    """Sessions shaped the way the relay opens them: synchronous, one per cycle."""
    return create_sync_sessionmaker(sync_engine)


@pytest.fixture
def run_async() -> RunAsync:
    """Run a coroutine that needs sessions, building the engine inside the loop.

    An async engine binds its pool to the loop that created it, so a shared engine
    would fail the moment a second ``asyncio.run`` opened a new one.
    """

    def _run[T](scenario: Callable[[SessionFactory], Awaitable[T]]) -> T:
        async def main() -> T:
            engine = create_async_engine_from_settings()
            try:
                return await scenario(create_sessionmaker(engine))
            finally:
                await engine.dispose()

        return asyncio.run(main())

    return _run


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

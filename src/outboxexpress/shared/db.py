"""Engine and session factories. Deliberately no module-level engine."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

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


def create_sync_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    # No expire_on_commit=False here, unlike the async factory: the relay reads Rows
    # from RETURNING rather than ORM instances, so expiry has nothing to reload.
    return sessionmaker(engine)

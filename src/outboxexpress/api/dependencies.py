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
# max_length matters as much as min_length: the key is a Text primary key, and a
# high-entropy key past Postgres's 2704-byte btree limit fails the INSERT as an
# OperationalError -- reported as 503 "database unavailable", which is both untrue
# and an invitation to retry forever. 255 is Stripe's documented limit.
IdempotencyKeyDep = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]

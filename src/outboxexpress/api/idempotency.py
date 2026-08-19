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
    # Hash the *validated* model, canonically serialized, so the comparison means
    # "same request" rather than "same bytes": key order and whitespace stop mattering.
    canonical = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def load_key(session: AsyncSession, key: str) -> IdempotencyKey | None:
    return await session.get(IdempotencyKey, key)

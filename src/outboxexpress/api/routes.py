"""The HTTP surface. Routes translate; they do not decide."""

from fastapi import APIRouter, status
from sqlalchemy import text

from .dependencies import IdempotencyKeyDep, SessionDep
from .responses import CanonicalJSONResponse
from .schemas import NewOrder, OrderResponse
from .service import place_order

router = APIRouter()


@router.get("/healthz")
async def healthz(session: SessionDep) -> dict[str, str]:
    # Postgres only. The API is healthy without a broker -- that is the pattern
    # working as intended, not a gap in the check (spec 11).
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


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
) -> CanonicalJSONResponse:
    stored = await place_order(session, idempotency_key=idempotency_key, request=order)
    # A raw response, deliberately, not a response_model: a replay must hand back the
    # body that was stored, and a response_model would rebuild it from a fresh object.
    return CanonicalJSONResponse(status_code=stored.status_code, content=stored.body)

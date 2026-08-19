"""The HTTP surface. Routes translate; they do not decide."""

from uuid import UUID

from fastapi import APIRouter, status
from sqlalchemy import text

from .dependencies import IdempotencyKeyDep, SessionDep
from .queries import load_order
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


@router.get("/orders/{order_id}", responses={404: {"description": "No order with that id"}})
async def read_order(session: SessionDep, order_id: UUID) -> OrderResponse:
    # A fresh representation, so `response_model` is right here -- unlike the write
    # path, which must hand back the bytes it stored.
    order = await load_order(session, order_id)
    return OrderResponse(order_id=order.id, status=order.status, created_at=order.created_at)

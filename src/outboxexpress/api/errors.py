"""Failures are handlers, not branches. Routes stay free of status codes."""

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from outboxexpress.shared.logging import get_logger

from .idempotency import IdempotencyConflict
from .queries import OrderNotFound

log = get_logger(__name__)

# Every handler takes `exc: Exception` because that is Starlette's registered
# handler signature; each narrows with an assert where it needs the real type.
# JSONResponse's first positional parameter is `content`, not `status_code`.


def register_error_handlers(app: FastAPI) -> None:
    """Map each failure the spec names to its status code, once, in one place."""
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(OrderNotFound, handle_order_not_found)
    app.add_exception_handler(IdempotencyConflict, handle_idempotency_conflict)
    app.add_exception_handler(OperationalError, handle_database_unavailable)


async def handle_validation_error(_: Request, exc: Exception) -> JSONResponse:
    """400, not FastAPI's default 422.

    The spec names 400 for both a malformed body and a missing Idempotency-Key,
    and FastAPI reports a missing required header as a validation error.
    """
    assert isinstance(exc, RequestValidationError)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"detail": jsonable_encoder(exc.errors())}
    )


async def handle_order_not_found(_: Request, exc: Exception) -> JSONResponse:
    """404. Naming the id makes the response useful in a log without a request trace."""
    assert isinstance(exc, OrderNotFound)
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})


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
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"detail": "database unavailable"}
    )

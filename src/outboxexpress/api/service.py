"""The write path. One transaction, three rows, no network call."""

from dataclasses import dataclass

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
    # The lookup sits inside the transaction: a read before session.begin() would open
    # an implicit one, and begin() would then raise "a transaction is already begun".
    async with session.begin():
        seen = await load_key(session, idempotency_key)
        if seen is not None:
            return _replay(seen, request)
        record = await _write(session, idempotency_key, request)
    # Readable after commit only because the sessionmaker sets expire_on_commit=False.
    log.info("order_committed", order_id=str(record.order_id))
    return StoredResponse(record.response_code, record.response_body)


async def _write(session: AsyncSession, idempotency_key: str, request: NewOrder) -> IdempotencyKey:
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
    log.info("order_written", order_id=str(order.id), event_id=str(event.event_id))
    # The order must land before the row whose foreign key references it. SQLAlchemy
    # sorts a flush by *relationship* dependency, and there is no relationship here --
    # only a raw ForeignKey column -- so the ordering is stated explicitly.
    session.add(order)
    await session.flush()
    session.add_all([outbox, record])
    return record


def _replay(record: IdempotencyKey, request: NewOrder) -> StoredResponse:
    if record.request_hash != request_fingerprint(request):
        raise IdempotencyConflict(record.key)
    return StoredResponse(record.response_code, record.response_body)

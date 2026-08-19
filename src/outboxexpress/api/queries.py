"""The read path. No transaction of its own -- a single statement is already atomic."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from outboxexpress.shared.models import Order


class OrderNotFound(Exception):
    """No order with that id."""

    def __init__(self, order_id: UUID) -> None:
        super().__init__(f"No order with id {order_id}")
        self.order_id = order_id


async def load_order(session: AsyncSession, order_id: UUID) -> Order:
    """Return the order, or raise ``OrderNotFound``.

    Raising rather than returning ``None`` keeps the absence decision here: the
    caller asks for an order and gets one, so no route has to branch on a null.
    """
    order = await session.get(Order, order_id)
    if order is None:
        raise OrderNotFound(order_id)
    return order

from uuid import UUID

import pytest

from conftest import RunAsync, SessionFactory
from outboxexpress.api.queries import OrderNotFound, load_order
from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order
from outboxexpress.shared.models import new_id

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


def test_load_order_returns_the_order_that_was_written(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[str, str, str, int]:
        async with factory() as session:
            stored = await place_order(session, idempotency_key="k-1", request=REQUEST)
        written_id = stored.body["order_id"]
        # A second session, so the read genuinely goes to Postgres rather than
        # finding the instance still in the first session's identity map.
        async with factory() as session:
            order = await load_order(session, UUID(written_id))
            return written_id, str(order.id), order.status, order.quantity

    written_id, loaded_id, status, quantity = run_async(scenario)
    assert loaded_id == written_id
    assert status == "CREATED"
    assert quantity == 2


def test_loading_an_unknown_order_raises(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> None:
        async with factory() as session:
            await load_order(session, new_id())

    with pytest.raises(OrderNotFound):
        run_async(scenario)

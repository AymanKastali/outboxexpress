"""I7: N concurrent identical posts create exactly one order and one event."""

import asyncio

from sqlalchemy import Engine, text

from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)
CONCURRENCY = 8


def test_concurrent_posts_with_one_key_create_one_order(run_async, sync_engine: Engine):
    async def scenario(factory):
        async def attempt():
            # A separate session per caller: a real race in Postgres, not a simulated
            # one. The losers block on the idempotency_keys primary key.
            async with factory() as session:
                return await place_order(session, idempotency_key="k-1", request=REQUEST)

        return await asyncio.gather(*(attempt() for _ in range(CONCURRENCY)))

    responses = run_async(scenario)

    assert len(responses) == CONCURRENCY
    assert {response.status_code for response in responses} == {201}
    # Every caller -- winner and losers alike -- sees the same body.
    assert len({response.body["order_id"] for response in responses}) == 1

    with sync_engine.connect() as connection:
        for table in ("orders", "outbox", "idempotency_keys"):
            count = connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            assert count == 1, f"{table} has {count} rows, expected 1"

"""I2: no event exists whose order is not committed, and no order lacks its event."""

import pytest
from sqlalchemy import Engine, text

from conftest import RunAsync, SessionFactory
from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import place_order, stage_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


class Boom(Exception):
    """Stands in for a process dying mid-transaction."""


def table_counts(engine: Engine) -> tuple[int, int]:
    with engine.connect() as connection:
        return (
            connection.execute(text("SELECT count(*) FROM orders")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM outbox")).scalar_one(),
        )


def test_a_failed_transaction_leaves_neither_an_order_nor_an_event(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    async def scenario(factory: SessionFactory) -> None:
        async with factory() as session:
            with pytest.raises(Boom):
                async with session.begin():
                    await stage_order(session, "k-1", REQUEST)
                    # Force all three INSERTs to the server, so the rollback below is
                    # undoing real rows rather than discarding a pending unit of work.
                    await session.flush()
                    raise Boom  # a crash after the inserts, before COMMIT

    run_async(scenario)
    assert table_counts(sync_engine) == (0, 0)


def test_every_committed_order_has_exactly_one_matching_event(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    async def scenario(factory: SessionFactory) -> None:
        for index in range(5):
            async with factory() as session:
                await place_order(session, idempotency_key=f"k-{index}", request=REQUEST)

    run_async(scenario)
    assert table_counts(sync_engine) == (5, 5)

    with sync_engine.connect() as connection:
        orphans = connection.execute(
            text(
                "SELECT count(*) FROM outbox o "
                "LEFT JOIN orders ord ON ord.id::text = o.aggregate_id "
                "WHERE ord.id IS NULL"
            )
        ).scalar_one()
        eventless = connection.execute(
            text(
                "SELECT count(*) FROM orders ord "
                "LEFT JOIN outbox o ON o.aggregate_id = ord.id::text "
                "WHERE o.id IS NULL"
            )
        ).scalar_one()
    assert orphans == 0
    assert eventless == 0

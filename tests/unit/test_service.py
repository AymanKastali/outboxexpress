import pytest
from sqlalchemy import Engine, text

from conftest import RunAsync, SessionFactory
from outboxexpress.api.idempotency import IdempotencyConflict
from outboxexpress.api.schemas import NewOrder
from outboxexpress.api.service import StoredResponse, place_order

REQUEST = NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=2)


def counts(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        return {
            table: connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            for table in ("orders", "outbox", "idempotency_keys")
        }


async def _place_one(factory: SessionFactory) -> StoredResponse:
    async with factory() as session:
        return await place_order(session, idempotency_key="k-1", request=REQUEST)


def test_one_call_writes_exactly_one_order_and_one_event(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    stored = run_async(_place_one)
    assert stored.status_code == 201
    assert counts(sync_engine) == {"orders": 1, "outbox": 1, "idempotency_keys": 1}


def test_the_outbox_row_carries_the_event_and_the_order_as_its_key(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    stored = run_async(_place_one)
    with sync_engine.connect() as connection:
        row = connection.execute(
            text("SELECT id, aggregate_type, aggregate_id, event_type, payload, status FROM outbox")
        ).one()
    assert row.aggregate_type == "order"
    assert row.aggregate_id == stored.body["order_id"]
    assert row.event_type == "OrderCreated"
    assert row.status == "pending"
    # spec 5.5: outbox.id IS the event_id
    assert row.payload["event_id"] == str(row.id)
    assert row.payload["order"]["id"] == stored.body["order_id"]


def test_replaying_a_key_returns_the_stored_response_byte_for_byte(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    async def scenario(factory: SessionFactory) -> tuple[StoredResponse, StoredResponse]:
        return await _place_one(factory), await _place_one(factory)

    first, second = run_async(scenario)
    assert first.body == second.body
    assert first.status_code == second.status_code == 201
    assert counts(sync_engine) == {"orders": 1, "outbox": 1, "idempotency_keys": 1}


def test_reusing_a_key_with_a_different_body_is_a_conflict(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> None:
        await _place_one(factory)
        async with factory() as session:
            await place_order(
                session,
                idempotency_key="k-1",
                request=NewOrder(customer_email="a@b.com", item_sku="SKU-1", quantity=99),
            )

    with pytest.raises(IdempotencyConflict):
        run_async(scenario)

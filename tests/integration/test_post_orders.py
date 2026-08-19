from typing import Any

from sqlalchemy import Engine, text

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client


def test_post_orders_creates_an_order(run_async: RunAsync, sync_engine: Engine) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any]]:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders", json=VALID_ORDER, headers={"Idempotency-Key": "k-1"}
            )
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 201
    assert body["status"] == "CREATED"
    assert body["order_id"]

    with sync_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM orders")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM outbox")).scalar_one() == 1

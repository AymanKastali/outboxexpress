from datetime import datetime
from typing import Any

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client
from outboxexpress.shared.models import new_id


def test_get_returns_the_order_that_was_posted(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any], dict[str, Any]]:
        async with api_client(factory) as client:
            created = await client.post(
                "/orders", json=VALID_ORDER, headers={"Idempotency-Key": "k-1"}
            )
            fetched = await client.get(f"/orders/{created.json()['order_id']}")
            return fetched.status_code, fetched.json(), created.json()

    status, body, created = run_async(scenario)
    assert status == 200
    assert body["order_id"] == created["order_id"]
    assert body["status"] == "CREATED"
    # Instants, not strings: the stored POST body spells UTC "+00:00" and this
    # endpoint spells it "Z". Same moment, two legal RFC 3339 renderings.
    assert datetime.fromisoformat(body["created_at"]) == datetime.fromisoformat(
        created["created_at"]
    )


def test_an_unknown_order_is_404(run_async: RunAsync) -> None:
    """The body is asserted on purpose.

    Before the route exists FastAPI answers an unknown path with its own 404, so
    a status-only assertion would pass against a route that was never written.
    Only our handler names the id.
    """
    missing_id = str(new_id())

    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, Any]]:
        async with api_client(factory) as client:
            response = await client.get(f"/orders/{missing_id}")
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 404
    assert missing_id in body["detail"]


def test_a_malformed_order_id_is_400(run_async: RunAsync) -> None:
    """FastAPI validates the path parameter, so a non-UUID never reaches Postgres.
    Drop the ``UUID`` annotation on the route and this returns 404 instead."""

    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            return (await client.get("/orders/not-a-uuid")).status_code

    assert run_async(scenario) == 400

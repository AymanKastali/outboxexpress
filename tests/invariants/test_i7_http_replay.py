from httpx import Response
from sqlalchemy import Engine, text

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client


def test_replay_returns_the_stored_bytes_unchanged(
    run_async: RunAsync, sync_engine: Engine
) -> None:
    """Invariant I7 at the HTTP edge: a retrying client cannot tell a replay
    from the original, and never learns its first attempt had already succeeded."""

    async def scenario(factory: SessionFactory) -> tuple[Response, Response]:
        async with api_client(factory) as client:
            headers = {"Idempotency-Key": "k-1"}
            first = await client.post("/orders", json=VALID_ORDER, headers=headers)
            second = await client.post("/orders", json=VALID_ORDER, headers=headers)
            return first, second

    first, second = run_async(scenario)

    assert first.status_code == second.status_code == 201
    assert first.content == second.content

    with sync_engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM orders")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM outbox")).scalar_one() == 1

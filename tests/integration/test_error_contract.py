import secrets

from sqlalchemy.ext.asyncio import create_async_engine

from conftest import VALID_ORDER, RunAsync, SessionFactory, api_client
from outboxexpress.shared.db import create_sessionmaker


def test_missing_idempotency_key_is_400(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            return (await client.post("/orders", json=VALID_ORDER)).status_code

    assert run_async(scenario) == 400


def test_invalid_body_is_400(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders",
                json={**VALID_ORDER, "quantity": 0},
                headers={"Idempotency-Key": "k-1"},
            )
            return response.status_code

    assert run_async(scenario) == 400


def test_reused_key_with_a_different_body_is_409(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            headers = {"Idempotency-Key": "k-1"}
            await client.post("/orders", json=VALID_ORDER, headers=headers)
            response = await client.post(
                "/orders", json={**VALID_ORDER, "quantity": 9}, headers=headers
            )
            return response.status_code

    assert run_async(scenario) == 409


def test_quantity_beyond_the_column_is_400(run_async: RunAsync) -> None:
    """A quantity larger than INTEGER is a client bug, so it is rejected at the
    edge. Unbounded, it reaches Postgres and raises DataError -- a 500."""

    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders",
                json={**VALID_ORDER, "quantity": 2**40},
                headers={"Idempotency-Key": "k-1"},
            )
            return response.status_code

    assert run_async(scenario) == 400


def test_oversized_idempotency_key_is_400(run_async: RunAsync) -> None:
    """The key is a Text primary key, and Postgres caps a btree entry at 2704
    bytes. High entropy matters: a repetitive key of the same length compresses
    and fits, so a lazy test here would pass while real keys fail."""

    async def scenario(factory: SessionFactory) -> int:
        async with api_client(factory) as client:
            response = await client.post(
                "/orders",
                json=VALID_ORDER,
                headers={"Idempotency-Key": secrets.token_hex(2000)},
            )
            return response.status_code

    assert run_async(scenario) == 400


def test_database_outage_is_503(run_async: RunAsync) -> None:
    """The one handler with no other coverage. It must stay reachable: a client
    reads 500 as "my request was malformed" and stops retrying."""

    async def scenario(_: SessionFactory) -> int:
        # Port 1 is never listening, so the connection is refused the way a real
        # outage refuses it -- no container to stop, no wait for a timeout.
        engine = create_async_engine("postgresql+psycopg://outbox:outbox@127.0.0.1:1/outbox")
        try:
            async with api_client(create_sessionmaker(engine)) as client:
                response = await client.post(
                    "/orders", json=VALID_ORDER, headers={"Idempotency-Key": "k-1"}
                )
                return response.status_code
        finally:
            await engine.dispose()

    assert run_async(scenario) == 503

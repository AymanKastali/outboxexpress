from conftest import RunAsync, SessionFactory, api_client


def test_healthz_reports_ok_when_postgres_is_reachable(run_async: RunAsync) -> None:
    async def scenario(factory: SessionFactory) -> tuple[int, dict[str, str]]:
        async with api_client(factory) as client:
            response = await client.get("/healthz")
            return response.status_code, response.json()

    status, body = run_async(scenario)
    assert status == 200
    assert body == {"status": "ok"}

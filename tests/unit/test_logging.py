import json

import pytest
import structlog

from outboxexpress.shared.logging import configure_logging, get_logger


def test_bound_event_id_appears_in_every_json_line(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(event_id="0199a0f0-0000-7000-8000-000000000000")
    try:
        get_logger("test").info("order_committed", order_id="abc")
    finally:
        structlog.contextvars.clear_contextvars()

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["event"] == "order_committed"
    assert payload["order_id"] == "abc"
    assert payload["event_id"] == "0199a0f0-0000-7000-8000-000000000000"
    assert payload["level"] == "info"

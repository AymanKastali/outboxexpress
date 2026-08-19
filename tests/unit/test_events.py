from datetime import UTC, datetime
from uuid import UUID

from outboxexpress.shared.events import OrderCreatedV1, OrderSnapshot


def test_json_shape_matches_the_wire_contract():
    event = OrderCreatedV1(
        event_id=UUID("0199a0f0-0000-7000-8000-000000000001"),
        occurred_at=datetime(2026, 8, 19, 7, 0, tzinfo=UTC),
        order=OrderSnapshot(
            id=UUID("0199a0f0-0000-7000-8000-000000000002"),
            customer_email="a@b.com",
            item_sku="SKU-1",
            quantity=2,
            created_at=datetime(2026, 8, 19, 7, 0, tzinfo=UTC),
        ),
    )
    payload = event.model_dump(mode="json")
    assert payload["event_type"] == "OrderCreated"
    assert payload["version"] == 1
    assert set(payload) == {"event_id", "event_type", "version", "occurred_at", "order"}
    assert set(payload["order"]) == {"id", "customer_email", "item_sku", "quantity", "created_at"}
    # JSON round-trip is what the relay and the notifier both depend on.
    assert OrderCreatedV1.model_validate(payload) == event

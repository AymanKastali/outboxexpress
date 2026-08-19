import pytest
from pydantic import ValidationError

from outboxexpress.api.idempotency import request_fingerprint
from outboxexpress.api.schemas import NewOrder


def order(customer_email: str = "a@b.com", item_sku: str = "SKU-1", quantity: int = 2) -> NewOrder:
    return NewOrder(customer_email=customer_email, item_sku=item_sku, quantity=quantity)


def test_fingerprint_is_stable_for_equal_requests() -> None:
    assert request_fingerprint(order()) == request_fingerprint(order())


def test_fingerprint_changes_when_any_field_changes() -> None:
    baseline = request_fingerprint(order())
    assert request_fingerprint(order(quantity=3)) != baseline
    assert request_fingerprint(order(item_sku="SKU-2")) != baseline


def test_fingerprint_is_a_sha256_hex_digest() -> None:
    assert len(request_fingerprint(order())) == 64


def test_invalid_requests_never_reach_the_database() -> None:
    with pytest.raises(ValidationError):
        order(quantity=0)
    with pytest.raises(ValidationError):
        order(customer_email="not-an-email")

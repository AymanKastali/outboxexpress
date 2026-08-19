"""The four tables. Every guarantee here is a constraint, not application code being careful."""

import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

ORDER_STATUS_CREATED = "CREATED"
AGGREGATE_TYPE_ORDER = "order"

# Deterministic constraint names, so Alembic autogenerate can always find them again.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> uuid.UUID:
    """UUIDv7 — random, but time-ordered, so primary keys append to the B-tree."""
    return uuid.uuid7()


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class OutboxStatus(enum.StrEnum):
    # StrEnum's auto() is the lowercased member name — the values the partial
    # index predicates in the migration match on.
    PENDING = enum.auto()
    PUBLISHING = enum.auto()
    PUBLISHED = enum.auto()
    DEAD = enum.auto()


_outbox_status = Enum(
    OutboxStatus,
    name="outbox_status",
    values_callable=lambda e: [member.value for member in e],
)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    customer_email: Mapped[str] = mapped_column(Text, nullable=False)
    item_sku: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default=ORDER_STATUS_CREATED)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )

    __table_args__ = (CheckConstraint("quantity > 0", name="quantity_positive"),)


class Outbox(Base):
    """The event above the line; relay bookkeeping below it."""

    __tablename__ = "outbox"

    # This id IS the event_id, carried unchanged to Kafka and on to the consumer.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )

    # --- relay state, private to the relay (M2) ---
    status: Mapped[OutboxStatus] = mapped_column(
        _outbox_status, nullable=False, default=OutboxStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )
    locked_by: Mapped[str | None] = mapped_column(Text)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_outbox_reclaim",
            "locked_until",
            postgresql_where=text("status = 'publishing'"),
        ),
    )


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"), nullable=False)
    response_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )


class ProcessedEvent(Base):
    """Consumer-side dedup (M3). Composite key so a second handler dedups independently."""

    __tablename__ = "processed_events"

    handler: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, server_default=func.now()
    )

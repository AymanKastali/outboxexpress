"""The wire contract. Imported by the writer and, from M2 on, by every reader."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OrderSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    customer_email: str
    item_sku: str
    quantity: int
    created_at: datetime


class OrderCreatedV1(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID
    event_type: Literal["OrderCreated"] = "OrderCreated"
    version: Literal[1] = 1
    occurred_at: datetime
    order: OrderSnapshot

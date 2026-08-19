from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class NewOrder(BaseModel):
    # forbid: an unrecognised field must be a client bug, not a silently dropped one.
    model_config = ConfigDict(extra="forbid", frozen=True)

    customer_email: EmailStr
    item_sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0)


class OrderResponse(BaseModel):
    order_id: UUID
    status: str
    created_at: datetime

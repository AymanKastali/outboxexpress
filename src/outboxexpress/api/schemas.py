from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field

# The quantity column is INTEGER. Bounding the field here makes an oversized
# quantity a 400 at the edge; unbounded, it reaches Postgres and raises DataError,
# which no handler covers and which a client would read as a server fault.
MAX_INT32 = 2_147_483_647


class NewOrder(BaseModel):
    # forbid: an unrecognized field must be a client bug, not a silently dropped one.
    model_config = ConfigDict(extra="forbid", frozen=True)

    customer_email: EmailStr
    item_sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0, le=MAX_INT32)


class OrderResponse(BaseModel):
    order_id: UUID
    status: str
    created_at: datetime

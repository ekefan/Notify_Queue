from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

class Channel(str, Enum):
    sms = "sms"
    email = "email"
    push = "push"

class Priority(int, Enum):
    low = 0
    normal = 1
    high = 2

class ScheduleJobReq(BaseModel):
    idempotency_key: str
    recipient: UUID
    channel: Channel
    payload: dict[str, Any]
    scheduled_for: datetime
    priority: Priority = Priority.normal

    @field_validator("scheduled_for")
    @classmethod
    def scheduled_for_must_be_future_or_now(cls, v:datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("scheduled_for must be timezone-aware")
        if v < datetime.now(timezone.utc).replace(year=datetime.now().year - 1):
            raise ValueError("scheduled_for is implausibly far in the past")
        return v

class ScheduledJobResp(BaseModel):
    id: UUID
    idempotency_key: str
    recipient: UUID
    channel: Channel
    status: str
    scheduled_for: datetime
    priority: Priority
    created_at: datetime
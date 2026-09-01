from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

class Channel(str, Enum):
    sms = "sms"
    email = "email"
    push = "push"

class Priority(int, Enum):
    low = 0
    normal = 1
    high = 2

class ScheduleJobReq(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    recipient: str = Field(min_length=1, max_length=512)
    channel: Channel
    payload: dict[str, Any]
    send_at: datetime
    priority: Priority = Priority.normal

    @field_validator("send_at")
    @classmethod
    def send_at_must_be_future_or_now(cls, v:datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("send_at must be timezone-aware")
        if v < datetime.now(timezone.utc).replace(year=datetime.now().year - 1):
            raise ValueError("send_at is implausibly far in the past")
        return v

class ScheduledJobResp(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    idempotency_key: str
    recipient: str
    channel: Channel
    payload: dict[str, Any]
    scheduled_for: datetime
    priority: Priority
    status: str
    attempts: int
    created_at: datetime


class ScheduleJobResp(ScheduledJobResp):
    deduplicated: bool


class JobStatusResp(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: str
    attempts: int
    max_attempts: int
    next_retry_at: datetime | None
    scheduled_for: datetime
    sent_at: datetime | None
    last_error: str | None

class WebhookEvent(BaseModel):
    job_id: UUID
    status: str
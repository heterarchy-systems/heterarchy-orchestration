from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from chattocodex.schemas.wake_schemas import WakeTargetId


class WakeDeliveryBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class WakeDeliveryLeaseRecord(WakeDeliveryBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    wake_revision: int = Field(ge=0)
    lease_started_at: datetime | None = None
    cooldown_until: datetime | None = None
    failure_streak: int = Field(ge=0)
    updated_at: datetime

    @field_validator("lease_started_at", "cooldown_until", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

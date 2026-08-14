from datetime import datetime
from typing import Literal

from pydantic import Field, field_validator

from chattocodex.schemas.main_runtime_schemas import (
    MainRuntimeBoundaryModel,
    MainTargetId,
)


class MainReviewDeliveryLeaseRecord(MainRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    main_target_id: MainTargetId
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

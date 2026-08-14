from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from chattocodex.schemas.wake_schemas import WakeTargetId


class WorkerRuntimeBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class WorkerRuntimePresenceRecord(WorkerRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    last_seen_at: datetime

    @field_validator("last_seen_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

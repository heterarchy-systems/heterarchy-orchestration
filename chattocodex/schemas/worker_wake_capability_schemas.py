from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from chattocodex.schemas.broker_schemas import WorkerSlotId
from chattocodex.schemas.orchestration_schemas import RunId
from chattocodex.schemas.project_schemas import ProjectId
from chattocodex.schemas.wake_schemas import WakeTargetId
from chattocodex.wake.worker_wake_capability_dto import (
    WorkerWakeCapabilityStatus,
    WorkerWakeTransport,
)


class WorkerWakeCapabilityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    project_id: ProjectId
    run_id: RunId
    registered_connector_epoch: int = Field(ge=0)
    wake_revision: int = Field(ge=1)
    transport: WorkerWakeTransport
    status: WorkerWakeCapabilityStatus
    bound_slot_id: WorkerSlotId | None
    revision: int = Field(ge=0)
    issued_at: datetime
    expires_at: datetime
    updated_at: datetime

    @field_validator("issued_at", "expires_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

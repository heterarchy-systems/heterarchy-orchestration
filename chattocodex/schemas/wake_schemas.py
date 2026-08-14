from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from chattocodex.schemas.broker_schemas import WorkerSlotId
from chattocodex.schemas.project_schemas import ProjectId
from chattocodex.wake.wake_dto import WakeTargetStatus

WakeTargetId = Annotated[
    str,
    StringConstraints(pattern=r"^wtgt_[0-9a-f]{32}$"),
]


class WakeBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class WakeTargetRecord(WakeBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    project_id: ProjectId
    registered_connector_epoch: int = Field(default=0, ge=0)
    status: WakeTargetStatus
    bound_slot_id: WorkerSlotId | None
    wake_revision: int = Field(ge=0)
    revision: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_busy_binding(self) -> WakeTargetRecord:
        if self.status is WakeTargetStatus.BUSY and self.bound_slot_id is None:
            raise ValueError("busy wake targets require a bound worker slot")
        return self


class WakePresenceRecord(WakeBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    last_seen_at: datetime

    @field_validator("last_seen_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value


class WakeProjectView(WakeBoundaryModel):
    project_id: ProjectId
    name: str


class WakeProjectsResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    projects: tuple[WakeProjectView, ...]


class WakeTargetRegisterRequest(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    project_id: ProjectId


class WakeTargetPollRequest(WakeBoundaryModel):
    wake_target_id: WakeTargetId


class WakeTargetRetireRequest(WakeBoundaryModel):
    expected_revision: int = Field(ge=0)


class WakeTargetRetirementItem(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    expected_revision: int = Field(ge=0)


class WakeTargetRetireBatchRequest(WakeBoundaryModel):
    project_id: ProjectId
    targets: tuple[WakeTargetRetirementItem, ...] = Field(
        min_length=1,
        max_length=32,
    )

    @field_validator("targets")
    @classmethod
    def require_unique_target_ids(
        cls, value: tuple[WakeTargetRetirementItem, ...]
    ) -> tuple[WakeTargetRetirementItem, ...]:
        target_ids = tuple(item.wake_target_id for item in value)
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("wake_target_id values must be unique")
        return value


class WakeTargetView(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    project_id: ProjectId
    registered_connector_epoch: int = Field(ge=0)
    status: WakeTargetStatus
    wake_revision: int = Field(ge=0)
    revision: int = Field(ge=0)
    updated_at: datetime


class WakeTargetResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    target: WakeTargetView


class WakeTargetRetireResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    wake_target_id: WakeTargetId
    retired_revision: int = Field(ge=0)


class WakeTargetRetireBatchResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    project_id: ProjectId
    retired_wake_target_ids: tuple[WakeTargetId, ...]

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

from chattocodex.main_runtime.main_review_target_state import MainReviewTargetStatus
from chattocodex.schemas.project_schemas import ProjectId
from chattocodex.schemas.run_control_schemas import (
    WorkflowHandle,
    WorkflowStatusSuccess,
)

MainTargetId = Annotated[
    str,
    StringConstraints(pattern=r"^mtgt_[0-9a-f]{32}$"),
]


class MainRuntimeBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class MainReviewTargetRecord(MainRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    main_target_id: MainTargetId
    project_id: ProjectId
    status: MainReviewTargetStatus
    pending_workflow_id: str | None = None
    pending_workflow_revision: int | None = Field(default=None, ge=0)
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
    def validate_pending_binding(self) -> MainReviewTargetRecord:
        has_workflow = self.pending_workflow_id is not None
        has_revision = self.pending_workflow_revision is not None
        if has_workflow != has_revision:
            raise ValueError("pending workflow identity must be complete")
        if self.status is MainReviewTargetStatus.IDLE and has_workflow:
            raise ValueError(
                "idle Main review targets cannot retain a pending workflow"
            )
        if self.status is not MainReviewTargetStatus.IDLE and not has_workflow:
            raise ValueError(
                "waking or active Main review targets require a pending workflow"
            )
        return self


class MainReviewTargetRegisterRequest(MainRuntimeBoundaryModel):
    main_target_id: MainTargetId
    project_id: ProjectId


class MainReviewTargetPollRequest(MainRuntimeBoundaryModel):
    main_target_id: MainTargetId


class MainReviewTargetView(MainRuntimeBoundaryModel):
    main_target_id: MainTargetId
    project_id: ProjectId
    status: MainReviewTargetStatus
    wake_revision: int = Field(ge=0)
    revision: int = Field(ge=0)
    updated_at: datetime


class MainReviewTargetResponse(MainRuntimeBoundaryModel):
    ok: Literal[True] = True
    target: MainReviewTargetView


class ResumeMainReviewRequest(MainRuntimeBoundaryModel):
    main_target_id: MainTargetId
    main_wake_revision: int = Field(ge=1)


class ResumeMainReviewSuccess(WorkflowStatusSuccess):
    main_target: MainReviewTargetView
    workflow_handle: WorkflowHandle

from datetime import datetime
from typing import Annotated, Literal

from pydantic import StringConstraints, field_validator, model_validator

from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.run_control_schemas import (
    WorkflowHandle,
    WorkflowStatusSuccess,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.element_collect.main_runtime_enums import MainReviewTargetStatus

MainTargetId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^mtgt_[0-9a-f]{32}$"),
]


class MainRuntimeBoundaryModel(StrictSchemaModel):
    pass


class MainReviewTargetRecord(MainRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    main_target_id: MainTargetId
    project_id: ProjectId
    status: MainReviewTargetStatus
    pending_workflow_id: str | None = None
    pending_workflow_revision: Annotated[
        int | None,
        described_field(
            description="Pending workflow revision for this main review target record.",
            ge=0,
        ),
    ] = None
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this main review target record.", ge=0
        ),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this main review target record.", ge=0
        ),
    ]
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_pending_binding(self) -> MainReviewTargetRecord:
        """Validate pending binding. Args: None. Returns: MainReviewTargetRecord."""
        status = MainReviewTargetStatus(self.status)
        has_workflow = self.pending_workflow_id is not None
        has_revision = self.pending_workflow_revision is not None
        if has_workflow != has_revision:
            raise ValueError("pending workflow identity must be complete")
        if status is MainReviewTargetStatus.IDLE and has_workflow:
            raise ValueError(
                "idle Main review targets cannot retain a pending workflow"
            )
        if status is not MainReviewTargetStatus.IDLE and not has_workflow:
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
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this main review target view.", ge=0
        ),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this main review target view.", ge=0
        ),
    ]
    updated_at: datetime


class MainReviewTargetResponse(MainRuntimeBoundaryModel):
    ok: Literal[True] = True
    target: MainReviewTargetView


class ResumeMainReviewRequest(MainRuntimeBoundaryModel):
    main_target_id: MainTargetId
    main_wake_revision: Annotated[
        int,
        described_field(
            description="Main wake revision for this resume main review request.", ge=1
        ),
    ]


class ResumeMainReviewSuccess(WorkflowStatusSuccess):
    main_target: MainReviewTargetView
    workflow_handle: WorkflowHandle

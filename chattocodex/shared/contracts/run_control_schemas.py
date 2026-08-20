from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator, model_validator

from chattocodex.shared.contracts.broker.broker_schemas import BrokerRunStatusSuccess
from chattocodex.shared.contracts.orchestration_schemas import GoalText
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.coordination.facade_types import (
    FacadeObjective,
    FacadeRole,
    TaskKey,
)
from chattocodex.shared.contracts.types.coordination.run_control_types import (
    ReviewInstructions,
    ReviewSummary,
    WorkflowHandle,
    WorkflowId,
)
from chattocodex.shared.element_collect.run_control_enums import (
    WorkflowDecision,
    WorkflowStatus,
)


class WorkflowBoundaryModel(StrictSchemaModel):
    pass


class WorkflowTaskInput(WorkflowBoundaryModel):
    key: TaskKey
    objective: FacadeObjective
    role: FacadeRole | None = None
    depends_on: tuple[TaskKey, ...] = ()


class WorkflowRoundRecord(WorkflowBoundaryModel):
    round_number: Annotated[
        int,
        described_field(
            description="Round number for this workflow round record.", ge=1
        ),
    ]
    run_id: str
    task_keys: tuple[TaskKey, ...]
    broker_task_ids: tuple[str, ...]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value


class WorkflowRecord(WorkflowBoundaryModel):
    schema_version: Literal[1] = 1
    workflow_id: WorkflowId
    workflow_handle_digest: str
    goal: GoalText
    project_id: ProjectId
    review_instructions: ReviewInstructions
    max_rework_rounds: Annotated[
        int,
        described_field(
            description="Maximum rework rounds allowed for this workflow record.",
            ge=0,
            le=8,
        ),
    ]
    rework_rounds: Annotated[
        int,
        described_field(
            description="Rework rounds for this workflow record.", ge=0, le=8
        ),
    ]
    status: WorkflowStatus
    rounds: Annotated[
        tuple[WorkflowRoundRecord, ...],
        described_field(description="Rounds for this workflow record.", min_length=1),
    ]
    last_review_summary: ReviewSummary | None = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this workflow record.", ge=0
        ),
    ]
    created_at: datetime
    updated_at: datetime
    requested_parallelism: Annotated[
        int,
        described_field(
            description="Requested parallelism for this workflow record.", ge=1, le=32
        ),
    ] = 1

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_round_count(self) -> WorkflowRecord:
        """Validate round count. Args: None. Returns: WorkflowRecord."""
        if self.rework_rounds != len(self.rounds) - 1:
            raise ValueError("rework_rounds must equal rounds minus the initial round")
        return self


class WorkflowRoundView(WorkflowBoundaryModel):
    round_number: int
    run_id: str
    task_keys: tuple[TaskKey, ...]
    broker_task_ids: tuple[str, ...]
    created_at: datetime


class WorkflowView(WorkflowBoundaryModel):
    workflow_id: WorkflowId
    goal: GoalText
    project_id: ProjectId
    review_instructions: ReviewInstructions
    max_rework_rounds: int
    rework_rounds: int
    status: WorkflowStatus
    rounds: tuple[WorkflowRoundView, ...]
    current_run_id: str
    last_review_summary: ReviewSummary | None
    revision: int
    created_at: datetime
    updated_at: datetime
    requested_parallelism: int


class OrchestrateWorkRequest(WorkflowBoundaryModel):
    goal: GoalText
    tasks: Annotated[
        tuple[WorkflowTaskInput, ...],
        described_field(
            description="Tasks for this orchestrate work request.",
            min_length=1,
            max_length=32,
        ),
    ]
    review_instructions: ReviewInstructions = (
        "Inspect all worker results, reports, diffs, and tests. Approve only when the goal is met; "
        "otherwise request bounded rework or fail the workflow."
    )
    max_rework_rounds: Annotated[
        int,
        described_field(
            description="Maximum rework rounds allowed for this orchestrate work request.",
            ge=0,
            le=8,
        ),
    ] = 2
    requested_parallelism: Annotated[
        int | None,
        described_field(
            description="Requested parallelism for this orchestrate work request.",
            ge=1,
            le=32,
        ),
    ] = None
    project_id: ProjectId | None = None


class OrchestrateWorkSuccess(WorkflowBoundaryModel):
    ok: Literal[True] = True
    workflow: WorkflowView
    workflow_handle: WorkflowHandle
    current_run: BrokerRunStatusSuccess
    needs_main_review: bool


class WorkflowStatusRequest(WorkflowBoundaryModel):
    workflow_id: WorkflowId
    workflow_handle: WorkflowHandle


class WorkflowStatusSuccess(WorkflowBoundaryModel):
    ok: Literal[True] = True
    workflow: WorkflowView
    current_run: BrokerRunStatusSuccess
    needs_main_review: bool


class ReviewWorkflowRequest(WorkflowBoundaryModel):
    workflow_id: WorkflowId
    workflow_handle: WorkflowHandle
    decision: WorkflowDecision
    review_summary: ReviewSummary
    rework_tasks: Annotated[
        tuple[WorkflowTaskInput, ...],
        described_field(
            description="Rework tasks for this review workflow request.", max_length=32
        ),
    ] = ()

    @model_validator(mode="after")
    def validate_rework_payload(self) -> ReviewWorkflowRequest:
        """Validate rework payload. Args: None. Returns: ReviewWorkflowRequest."""
        decision = WorkflowDecision(self.decision)
        if decision is WorkflowDecision.REWORK and not self.rework_tasks:
            raise ValueError("rework decision requires at least one rework task")
        if decision is not WorkflowDecision.REWORK and self.rework_tasks:
            raise ValueError("rework_tasks are allowed only for a rework decision")
        return self


class ReviewWorkflowSuccess(WorkflowStatusSuccess):
    decision: WorkflowDecision
    new_round_started: bool

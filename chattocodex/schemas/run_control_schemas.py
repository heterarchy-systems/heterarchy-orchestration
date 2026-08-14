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

from chattocodex.facade.facade_schemas import FacadeObjective, FacadeRole, TaskKey
from chattocodex.run_control.run_control_state import WorkflowDecision, WorkflowStatus
from chattocodex.schemas.broker_schemas import BrokerRunStatusSuccess
from chattocodex.schemas.orchestration_schemas import GoalText
from chattocodex.schemas.project_schemas import ProjectId

WorkflowId = Annotated[str, StringConstraints(pattern=r"^wf_[0-9a-f]{24}$")]
WorkflowHandle = Annotated[
    str,
    StringConstraints(pattern=r"^wfh_[A-Za-z0-9_-]{20,}$", max_length=96),
]
ReviewInstructions = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=8_000),
]
ReviewSummary = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=16_000),
]


class WorkflowBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class WorkflowTaskInput(WorkflowBoundaryModel):
    key: TaskKey
    objective: FacadeObjective
    role: FacadeRole | None = None
    depends_on: tuple[TaskKey, ...] = ()


class WorkflowRoundRecord(WorkflowBoundaryModel):
    round_number: int = Field(ge=1)
    run_id: str
    task_keys: tuple[TaskKey, ...]
    broker_task_ids: tuple[str, ...]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
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
    max_rework_rounds: int = Field(ge=0, le=8)
    rework_rounds: int = Field(ge=0, le=8)
    status: WorkflowStatus
    rounds: tuple[WorkflowRoundRecord, ...] = Field(min_length=1)
    last_review_summary: ReviewSummary | None = None
    revision: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime
    requested_parallelism: int = Field(default=1, ge=1, le=32)

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_round_count(self) -> WorkflowRecord:
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
    tasks: tuple[WorkflowTaskInput, ...] = Field(min_length=1, max_length=32)
    review_instructions: ReviewInstructions = (
        "Inspect all worker results, reports, diffs, and tests. Approve only when the goal is met; "
        "otherwise request bounded rework or fail the workflow."
    )
    max_rework_rounds: int = Field(default=2, ge=0, le=8)
    requested_parallelism: int | None = Field(default=None, ge=1, le=32)
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
    rework_tasks: tuple[WorkflowTaskInput, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def validate_rework_payload(self) -> ReviewWorkflowRequest:
        if self.decision is WorkflowDecision.REWORK and not self.rework_tasks:
            raise ValueError("rework decision requires at least one rework task")
        if self.decision is not WorkflowDecision.REWORK and self.rework_tasks:
            raise ValueError("rework_tasks are allowed only for a rework decision")
        return self


class ReviewWorkflowSuccess(WorkflowStatusSuccess):
    decision: WorkflowDecision
    new_round_started: bool

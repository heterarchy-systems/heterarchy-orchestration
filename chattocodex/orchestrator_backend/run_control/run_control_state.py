from dataclasses import dataclass
from datetime import datetime

from chattocodex.orchestrator_backend.broker.broker_dto import BrokerRunSnapshot
from chattocodex.shared.element_collect.run_control_enums import (
    WorkflowDecision,
    WorkflowStatus,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowTaskSpec:
    key: str
    objective: str
    role: str | None
    depends_on: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowRoundState:
    round_number: int
    run_id: str
    task_keys: tuple[str, ...]
    broker_task_ids: tuple[str, ...]
    created_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowState:
    schema_version: int
    workflow_id: str
    workflow_handle_digest: str
    goal: str
    project_id: str
    review_instructions: str
    max_rework_rounds: int
    rework_rounds: int
    status: WorkflowStatus
    rounds: tuple[WorkflowRoundState, ...]
    last_review_summary: str | None
    revision: int
    created_at: datetime
    updated_at: datetime
    requested_parallelism: int = 1

    @property
    def current_round(self) -> WorkflowRoundState:
        """Current round. Args: None. Returns: WorkflowRoundState."""
        return self.rounds[-1]


@dataclass(frozen=True, slots=True, kw_only=True)
class CreatedWorkflow:
    state: WorkflowState
    workflow_handle: str


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowSnapshot:
    workflow: WorkflowState
    current_run: BrokerRunSnapshot
    needs_main_review: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowReviewResult:
    snapshot: WorkflowSnapshot
    decision: WorkflowDecision
    new_round_started: bool

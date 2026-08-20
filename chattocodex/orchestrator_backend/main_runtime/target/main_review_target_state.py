from dataclasses import dataclass
from datetime import datetime

from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowSnapshot,
)
from chattocodex.shared.element_collect.main_runtime_enums import MainReviewTargetStatus


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewTargetState:
    schema_version: int
    main_target_id: str
    project_id: str
    status: MainReviewTargetStatus
    pending_workflow_id: str | None
    pending_workflow_revision: int | None
    wake_revision: int
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewResume:
    target: MainReviewTargetState
    workflow: WorkflowSnapshot
    workflow_handle: str

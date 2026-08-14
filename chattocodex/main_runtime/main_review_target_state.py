from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from chattocodex.run_control.run_control_state import WorkflowSnapshot


class MainReviewTargetStatus(StrEnum):
    IDLE = "idle"
    WAKING = "waking"
    ACTIVE = "active"


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

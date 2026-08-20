from dataclasses import dataclass

from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowSnapshot,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewCapability:
    snapshot: WorkflowSnapshot
    workflow_handle: str

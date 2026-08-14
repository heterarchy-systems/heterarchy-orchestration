from dataclasses import dataclass

from chattocodex.run_control.run_control_state import WorkflowSnapshot


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewCapability:
    snapshot: WorkflowSnapshot
    workflow_handle: str

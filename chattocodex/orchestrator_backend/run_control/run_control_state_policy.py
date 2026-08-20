from datetime import datetime

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
)
from chattocodex.orchestrator_backend.run_control.run_control_identity import (
    capability_matches,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowRoundState,
    WorkflowState,
    WorkflowStatus,
)


def transition_workflow_state(
    state: WorkflowState,
    status: WorkflowStatus,
    last_review_summary: str | None,
    rounds: tuple[WorkflowRoundState, ...],
    rework_rounds: int,
    now: datetime,
) -> WorkflowState:
    """Build the next immutable workflow state revision. Args: state, status, last_review_summary, rounds, rework_rounds, now. Returns: WorkflowState."""

    return WorkflowState(
        schema_version=state.schema_version,
        workflow_id=state.workflow_id,
        workflow_handle_digest=state.workflow_handle_digest,
        goal=state.goal,
        project_id=state.project_id,
        review_instructions=state.review_instructions,
        max_rework_rounds=state.max_rework_rounds,
        rework_rounds=rework_rounds,
        status=status,
        rounds=rounds,
        last_review_summary=last_review_summary,
        revision=state.revision + 1,
        created_at=state.created_at,
        updated_at=now,
        requested_parallelism=state.requested_parallelism,
    )


def require_workflow_capability(
    state: WorkflowState,
    workflow_handle: str,
) -> None:
    """Reject a workflow capability that does not match the persisted digest. Args: state, workflow_handle. Returns: None."""

    if not capability_matches(
        handle=workflow_handle,
        expected_digest=state.workflow_handle_digest,
    ):
        raise CapabilityDeniedError(
            operation="workflow_capability",
            resource_id=state.workflow_id,
        )

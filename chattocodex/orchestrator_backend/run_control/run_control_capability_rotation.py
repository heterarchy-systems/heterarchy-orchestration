from datetime import datetime

from chattocodex.orchestrator_backend.run_control.run_control_state import WorkflowState


def rotate_workflow_capability(
    state: WorkflowState,
    workflow_handle_digest: str,
    now: datetime,
) -> WorkflowState:
    """Rotate workflow capability. Args: state, workflow_handle_digest, now. Returns: WorkflowState."""
    return WorkflowState(
        schema_version=state.schema_version,
        workflow_id=state.workflow_id,
        workflow_handle_digest=workflow_handle_digest,
        goal=state.goal,
        project_id=state.project_id,
        review_instructions=state.review_instructions,
        max_rework_rounds=state.max_rework_rounds,
        rework_rounds=state.rework_rounds,
        status=state.status,
        rounds=state.rounds,
        last_review_summary=state.last_review_summary,
        revision=state.revision + 1,
        created_at=state.created_at,
        updated_at=now,
    )

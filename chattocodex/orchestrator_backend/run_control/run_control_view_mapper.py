from chattocodex.orchestrator_backend.broker.broker_view_mapper import (
    build_broker_run_status_success,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowReviewResult,
    WorkflowSnapshot,
    WorkflowState,
)
from chattocodex.shared.contracts.run_control_schemas import (
    ReviewWorkflowSuccess,
    WorkflowRoundView,
    WorkflowStatusSuccess,
    WorkflowView,
)


def build_workflow_view(state: WorkflowState) -> WorkflowView:
    """Build workflow view. Args: state. Returns: WorkflowView."""
    return WorkflowView(
        workflow_id=state.workflow_id,
        goal=state.goal,
        project_id=state.project_id,
        review_instructions=state.review_instructions,
        max_rework_rounds=state.max_rework_rounds,
        rework_rounds=state.rework_rounds,
        status=state.status,
        rounds=tuple(
            WorkflowRoundView(
                round_number=item.round_number,
                run_id=item.run_id,
                task_keys=item.task_keys,
                broker_task_ids=item.broker_task_ids,
                created_at=item.created_at,
            )
            for item in state.rounds
        ),
        current_run_id=state.current_round.run_id,
        last_review_summary=state.last_review_summary,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
        requested_parallelism=state.requested_parallelism,
    )


def build_workflow_status(snapshot: WorkflowSnapshot) -> WorkflowStatusSuccess:
    """Build workflow status. Args: snapshot. Returns: WorkflowStatusSuccess."""
    return WorkflowStatusSuccess(
        workflow=build_workflow_view(state=snapshot.workflow),
        current_run=build_broker_run_status_success(snapshot=snapshot.current_run),
        needs_main_review=snapshot.needs_main_review,
    )


def build_review_success(result: WorkflowReviewResult) -> ReviewWorkflowSuccess:
    """Build review success. Args: result. Returns: ReviewWorkflowSuccess."""
    status = build_workflow_status(snapshot=result.snapshot)
    return ReviewWorkflowSuccess(
        workflow=status.workflow,
        current_run=status.current_run,
        needs_main_review=status.needs_main_review,
        decision=result.decision,
        new_round_started=result.new_round_started,
    )

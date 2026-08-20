from pydantic import ValidationError

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    TaskState,
    WorkerContextSnapshot,
    WorkerState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)
from chattocodex.shared.contracts.orchestration_schemas import (
    RunView,
    TaskView,
    ToolError,
    WorkerContextSuccess,
    WorkerView,
)


def build_run_view(state: RunState) -> RunView:
    """Build run view. Args: state. Returns: RunView."""
    return RunView(
        run_id=state.run_id,
        goal=state.goal,
        project_id=state.project_id,
        project_name=state.project_name,
        project_root=state.project_root,
        status=state.status,
        revision=state.revision,
        requested_parallelism=state.requested_parallelism,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def build_worker_view(state: WorkerState) -> WorkerView:
    """Build worker view. Args: state. Returns: WorkerView."""
    return WorkerView(
        run_id=state.run_id,
        worker_id=state.worker_id,
        status=state.status,
        role=state.role,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def build_task_view(state: TaskState) -> TaskView:
    """Build task view. Args: state. Returns: TaskView."""
    return TaskView(
        run_id=state.run_id,
        task_id=state.task_id,
        assigned_worker_id=state.assigned_worker_id,
        objective=state.objective,
        status=state.status,
        revision=state.revision,
        progress_summary=state.progress_summary,
        result=state.result,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def build_worker_context_success(
    snapshot: WorkerContextSnapshot,
) -> WorkerContextSuccess:
    """Build worker context success. Args: snapshot. Returns: WorkerContextSuccess."""
    return WorkerContextSuccess(
        run=build_run_view(state=snapshot.run),
        worker=build_worker_view(state=snapshot.worker),
        tasks=tuple(build_task_view(state=task) for task in snapshot.tasks),
    )


def build_tool_error(error: OrchestrationError) -> ToolError:
    """Build tool error. Args: error. Returns: ToolError."""
    return ToolError(
        error_code=error.error_code,
        message=error.message,
        operation=error.operation,
        resource_id=error.resource_id,
        recoverable=error.recoverable,
        current_revision=error.current_revision,
    )


def build_validation_error(operation: str, error: ValidationError) -> ToolError:
    """Build validation error. Args: operation, error. Returns: ToolError."""
    return ToolError(
        error_code=OrchestrationErrorCode.VALIDATION_ERROR,
        message=f"Input validation failed for {operation}: {error.error_count()} error(s).",
        operation=operation,
        resource_id=None,
        recoverable=False,
        current_revision=None,
    )

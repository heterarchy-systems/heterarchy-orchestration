from datetime import UTC, datetime

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    TaskState,
    TaskStatus,
    WorkerContextSnapshot,
    WorkerState,
    WorkerStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    AssignmentMismatchError,
    CapabilityDeniedError,
    InvalidTransitionError,
    RevisionConflictError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_identity import (
    capability_matches,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_ALLOWED_TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.IN_PROGRESS: frozenset(
        {
            TaskStatus.IN_PROGRESS,
            TaskStatus.BLOCKED,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.BLOCKED: frozenset(
        {
            TaskStatus.BLOCKED,
            TaskStatus.IN_PROGRESS,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.COMPLETED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


class WorkerExecutionService:
    def __init__(
        self,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        task_repository: TaskStateRepository,
        lock_registry: EntityLockRegistry,
    ) -> None:
        """Initialize this object. Args: run_repository, worker_repository, task_repository, lock_registry. Returns: None."""
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._task_repository = task_repository
        self._lock_registry = lock_registry

    def get_worker_context(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> WorkerContextSnapshot:
        """Get worker context. Args: run_id, worker_id, worker_handle. Returns: WorkerContextSnapshot."""
        run = self._run_repository.read(run_id=run_id)
        worker = self._require_worker_capability(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
        )
        tasks = self._task_repository.list_for_worker(
            run_id=run_id,
            worker_id=worker_id,
        )
        return WorkerContextSnapshot(run=run, worker=worker, tasks=tasks)

    def update_task(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        task_id: str,
        expected_revision: int,
        status: TaskStatus,
        progress_summary: str,
        result: str | None,
    ) -> TaskState:
        """Update task. Args: run_id, worker_id, worker_handle, task_id, expected_revision, status, progress_summary, result. Returns: TaskState."""
        worker = self._require_worker_capability(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
        )
        if worker.status is not WorkerStatus.ACTIVE:
            raise InvalidTransitionError(
                operation="update_task",
                resource_id=worker_id,
                message="Worker must be active before updating its Task.",
            )

        entity_key = f"task:{run_id}:{task_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            task = self._task_repository.read(run_id=run_id, task_id=task_id)
            if task.assigned_worker_id != worker_id:
                raise AssignmentMismatchError(
                    operation="update_task",
                    resource_id=task_id,
                )
            self._require_revision(
                operation="update_task",
                resource_id=task_id,
                expected_revision=expected_revision,
                current_revision=task.revision,
            )
            self._require_task_transition(task=task, next_status=status, result=result)
            now = datetime.now(UTC)
            updated_task = TaskState(
                schema_version=task.schema_version,
                run_id=task.run_id,
                task_id=task.task_id,
                assigned_worker_id=task.assigned_worker_id,
                objective=task.objective,
                status=status,
                revision=task.revision + 1,
                progress_summary=progress_summary,
                result=result,
                created_at=task.created_at,
                updated_at=now,
            )
            return self._task_repository.write(
                state=updated_task,
                expected_revision=task.revision,
            )

    def _require_worker_capability(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> WorkerState:
        """Require worker capability. Args: run_id, worker_id, worker_handle. Returns: WorkerState."""
        worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
        if not capability_matches(
            handle=worker_handle,
            expected_digest=worker.worker_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="validate_worker_capability",
                resource_id=worker_id,
            )
        return worker

    def _require_revision(
        self,
        operation: str,
        resource_id: str,
        expected_revision: int,
        current_revision: int,
    ) -> None:
        """Require revision. Args: operation, resource_id, expected_revision, current_revision. Returns: None."""
        if expected_revision != current_revision:
            raise RevisionConflictError(
                operation=operation,
                resource_id=resource_id,
                current_revision=current_revision,
            )

    def _require_task_transition(
        self,
        task: TaskState,
        next_status: TaskStatus,
        result: str | None,
    ) -> None:
        """Require task transition. Args: task, next_status, result. Returns: None."""
        allowed_statuses = _ALLOWED_TASK_TRANSITIONS[task.status]
        if next_status not in allowed_statuses:
            raise InvalidTransitionError(
                operation="update_task",
                resource_id=task.task_id,
                message=(
                    f"Task transition {task.status.value!r} -> "
                    f"{next_status.value!r} is not allowed."
                ),
            )
        if next_status.is_terminal and result is None:
            raise InvalidTransitionError(
                operation="update_task",
                resource_id=task.task_id,
                message="Terminal task updates require a result.",
            )
        if not next_status.is_terminal and result is not None:
            raise InvalidTransitionError(
                operation="update_task",
                resource_id=task.task_id,
                message="Non-terminal task updates must not include a result.",
            )

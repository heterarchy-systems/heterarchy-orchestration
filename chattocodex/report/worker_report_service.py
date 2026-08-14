from datetime import UTC, datetime

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import (
    TaskState,
    TaskStatus,
    WorkerStatus,
)
from chattocodex.orchestration.orchestration_exceptions import (
    AssignmentMismatchError,
    CapabilityDeniedError,
    InvalidTransitionError,
    ResourceAlreadyExistsError,
    RevisionConflictError,
)
from chattocodex.orchestration.orchestration_identity import capability_matches
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.worker_report_repository import WorkerReportRepository
from chattocodex.report.worker_report_dto import WorkerReport

_REPORT_RESULT_PREFIX = "worker_report:"


class WorkerReportService:
    def __init__(
        self,
        *,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        task_repository: TaskStateRepository,
        report_repository: WorkerReportRepository,
        lock_registry: EntityLockRegistry,
    ) -> None:
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._task_repository = task_repository
        self._report_repository = report_repository
        self._lock_registry = lock_registry

    def submit(
        self,
        *,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        task_id: str,
        expected_revision: int,
        title: str,
        summary: str,
        content: str,
        references: tuple[str, ...],
    ) -> WorkerReport:
        self._run_repository.read(run_id=run_id)
        worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
        if not capability_matches(
            handle=worker_handle,
            expected_digest=worker.worker_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="worker_report_submit",
                resource_id=worker_id,
            )
        if worker.status is not WorkerStatus.ACTIVE:
            raise InvalidTransitionError(
                operation="worker_report_submit",
                resource_id=worker_id,
                message="Worker must claim or join the Run before submitting a report.",
            )

        entity_key = f"task:{run_id}:{task_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            task = self._task_repository.read(run_id=run_id, task_id=task_id)
            if task.assigned_worker_id != worker_id:
                raise AssignmentMismatchError(
                    operation="worker_report_submit",
                    resource_id=task_id,
                )

            existing = self._report_repository.read(run_id=run_id, task_id=task_id)
            if existing is not None:
                self._require_same_payload(
                    existing=existing,
                    worker_id=worker_id,
                    title=title,
                    summary=summary,
                    content=content,
                    references=references,
                )
                if self._is_completed_report_task(task=task):
                    return existing
                self._require_reportable_task(
                    task=task,
                    expected_revision=expected_revision,
                )
                self._complete_task(task=task, summary=summary)
                return existing

            self._require_reportable_task(
                task=task,
                expected_revision=expected_revision,
            )
            report = self._report_repository.save(
                report=WorkerReport(
                    run_id=run_id,
                    worker_id=worker_id,
                    task_id=task_id,
                    title=title,
                    summary=summary,
                    content=content,
                    references=references,
                    created_at=datetime.now(UTC),
                )
            )
            try:
                self._complete_task(task=task, summary=summary)
            except BaseException:
                self._report_repository.delete_if_matches(report=report)
                raise
            return report

    def list_for_run(self, *, run_id: str) -> tuple[WorkerReport, ...]:
        self._run_repository.read(run_id=run_id)
        return self._report_repository.list_for_run(run_id=run_id)

    def _require_reportable_task(
        self,
        *,
        task: TaskState,
        expected_revision: int,
    ) -> None:
        if task.revision != expected_revision:
            raise RevisionConflictError(
                operation="worker_report_submit",
                resource_id=task.task_id,
                current_revision=task.revision,
            )
        if task.status.is_terminal:
            raise InvalidTransitionError(
                operation="worker_report_submit",
                resource_id=task.task_id,
                message="A terminal Task cannot accept a new Worker report.",
            )

    def _complete_task(self, *, task: TaskState, summary: str) -> TaskState:
        now = datetime.now(UTC)
        return self._task_repository.write(
            state=TaskState(
                schema_version=task.schema_version,
                run_id=task.run_id,
                task_id=task.task_id,
                assigned_worker_id=task.assigned_worker_id,
                objective=task.objective,
                status=TaskStatus.COMPLETED,
                revision=task.revision + 1,
                progress_summary=summary,
                result=f"{_REPORT_RESULT_PREFIX}{task.task_id}",
                created_at=task.created_at,
                updated_at=now,
            ),
            expected_revision=task.revision,
        )

    def _is_completed_report_task(self, *, task: TaskState) -> bool:
        return (
            task.status is TaskStatus.COMPLETED
            and task.result == f"{_REPORT_RESULT_PREFIX}{task.task_id}"
        )

    def _require_same_payload(
        self,
        *,
        existing: WorkerReport,
        worker_id: str,
        title: str,
        summary: str,
        content: str,
        references: tuple[str, ...],
    ) -> None:
        if (
            existing.worker_id == worker_id
            and existing.title == title
            and existing.summary == summary
            and existing.content == content
            and existing.references == references
        ):
            return
        raise ResourceAlreadyExistsError(
            operation="worker_report_submit",
            resource_id=existing.task_id,
        )

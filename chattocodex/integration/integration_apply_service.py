from datetime import UTC, datetime
from pathlib import Path

from chattocodex.broker.broker_dto import BrokerTaskStatus
from chattocodex.integration.git_integration_runner import GitIntegrationRunner
from chattocodex.integration.integration_dto import IntegrationApplyResult
from chattocodex.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import (
    RunState,
    RunStatus,
    TaskStatus,
)
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.orchestration_identity import capability_matches
from chattocodex.persistence.broker_repository import BrokerTaskRepository
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
)
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.persistence.worker_result_repository import WorkerResultRepository
from chattocodex.run_control.run_control_state import WorkflowStatus


class IntegrationApplyService:
    def __init__(
        self,
        *,
        run_repository: RunStateRepository,
        task_repository: TaskStateRepository,
        broker_task_repository: BrokerTaskRepository,
        result_repository: WorkerResultRepository,
        preview_service: IntegrationPreviewService,
        runner: GitIntegrationRunner,
        integration_root: Path,
        lock_registry: EntityLockRegistry,
        run_control_state_store: RunControlStateStore | None = None,
    ) -> None:
        self._run_repository = run_repository
        self._task_repository = task_repository
        self._broker_task_repository = broker_task_repository
        self._result_repository = result_repository
        self._preview_service = preview_service
        self._runner = runner
        self._integration_root = integration_root.expanduser().resolve()
        self._lock_registry = lock_registry
        self._run_control_state_store = run_control_state_store

    def apply(self, *, run_id: str, run_handle: str) -> IntegrationApplyResult:
        run = self._run_repository.read(run_id=run_id)
        if not capability_matches(
            handle=run_handle,
            expected_digest=run.run_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="integration_apply",
                resource_id=run_id,
            )

        entity_key = f"integration:{run_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            if self._run_control_state_store is not None:
                workflows = self._run_control_state_store.list_for_run(run_id=run_id)
                if any(
                    workflow.status is not WorkflowStatus.COMPLETED
                    for workflow in workflows
                ):
                    raise InvalidTransitionError(
                        operation="integration_apply",
                        resource_id=run_id,
                        message=(
                            "Run belongs to a coordinated workflow that has not been approved by "
                            "the Main Review Gate."
                        ),
                    )

            broker_tasks = self._broker_task_repository.list_for_run(run_id=run_id)
            if not broker_tasks or any(
                task.status is not BrokerTaskStatus.COMPLETED for task in broker_tasks
            ):
                raise InvalidTransitionError(
                    operation="integration_apply",
                    resource_id=run_id,
                    message="All Broker Tasks must be completed before integrating Worker results.",
                )

            tasks = self._task_repository.list_for_run(run_id=run_id)
            if not tasks or any(
                task.status is not TaskStatus.COMPLETED for task in tasks
            ):
                raise InvalidTransitionError(
                    operation="integration_apply",
                    resource_id=run_id,
                    message="All Run tasks must be completed before integrating Worker results.",
                )

            preview = self._preview_service.preview(run_id=run_id)
            if preview.base_commit is None or preview.result_count == 0:
                raise InvalidTransitionError(
                    operation="integration_apply",
                    resource_id=run_id,
                    message="The Run has no Worker result commits to integrate.",
                )
            if not preview.ready_to_integrate:
                raise InvalidTransitionError(
                    operation="integration_apply",
                    resource_id=run_id,
                    message="Worker result paths overlap; resolve conflicts before integration.",
                )

            results = self._result_repository.list_for_run(run_id=run_id)
            execution = self._runner.apply(
                project_root=Path(run.project_root).resolve(),
                integration_root=self._integration_root / run_id,
                base_commit=preview.base_commit,
                result_commits=tuple(result.commit_sha for result in results),
                summary=f"Integrate ChatToCodex Run: {run.goal}"[:2000],
            )
            if run.status is RunStatus.ACTIVE:
                run = self._run_repository.write(
                    state=RunState(
                        schema_version=run.schema_version,
                        run_id=run.run_id,
                        goal=run.goal,
                        project_id=run.project_id,
                        project_name=run.project_name,
                        project_root=run.project_root,
                        run_handle_digest=run.run_handle_digest,
                        status=RunStatus.COMPLETED,
                        revision=run.revision + 1,
                        created_at=run.created_at,
                        updated_at=datetime.now(UTC),
                    )
                )
            return IntegrationApplyResult(
                run_id=run_id,
                base_commit=preview.base_commit,
                integration_commit=execution.integration_commit,
                result_count=len(results),
                changed_paths=execution.changed_paths,
            )

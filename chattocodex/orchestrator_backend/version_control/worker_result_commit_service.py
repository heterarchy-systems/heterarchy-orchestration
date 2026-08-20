from datetime import UTC, datetime
from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    WorkerStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_identity import (
    capability_matches,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.persistence.version_control.git_worktree_repository import (
    GitWorktreeRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_result_repository import (
    WorkerResultRepository,
)
from chattocodex.orchestrator_backend.version_control.git_exceptions import (
    GitWorktreeError,
)
from chattocodex.orchestrator_backend.version_control.git_result_commit_runner import (
    GitResultCommitRunner,
)
from chattocodex.orchestrator_backend.version_control.git_result_dto import (
    WorkerResultCommit,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class WorkerResultCommitService:
    def __init__(
        self,
        worker_repository: WorkerStateRepository,
        worktree_repository: GitWorktreeRepository,
        result_repository: WorkerResultRepository,
        commit_runner: GitResultCommitRunner,
        lock_registry: EntityLockRegistry,
    ) -> None:
        """Initialize this object. Args: worker_repository, worktree_repository, result_repository, commit_runner, lock_registry. Returns: None."""
        self._worker_repository = worker_repository
        self._worktree_repository = worktree_repository
        self._result_repository = result_repository
        self._commit_runner = commit_runner
        self._lock_registry = lock_registry

    def commit(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        summary: str,
    ) -> WorkerResultCommit:
        """Commit. Args: run_id, worker_id, worker_handle, summary. Returns: WorkerResultCommit."""
        entity_key = f"worker-workspace:{run_id}:{worker_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
            if not capability_matches(
                handle=worker_handle,
                expected_digest=worker.worker_handle_digest,
            ):
                raise CapabilityDeniedError(
                    operation="worker_result_commit",
                    resource_id=worker_id,
                )
            if worker.status is not WorkerStatus.ACTIVE:
                raise InvalidTransitionError(
                    operation="worker_result_commit",
                    resource_id=worker_id,
                    message="Worker must join the Run before committing a result.",
                )
            workspace = self._worktree_repository.read_worker_worktree(
                run_id=run_id,
                worker_id=worker_id,
            )
            if workspace is None:
                raise GitWorktreeError(
                    operation="worker_result_commit",
                    resource_id=worker_id,
                    message=(
                        "Worker write workspace is not prepared. "
                        "Call worker_workspace_prepare first."
                    ),
                    recoverable=True,
                )
            worktree_root = Path(workspace.worktree_root)
            existing = self._result_repository.read(
                run_id=run_id,
                worker_id=worker_id,
            )
            if existing is not None:
                self._reconcile_existing_result(
                    result=existing,
                    worktree_root=worktree_root,
                    recorded_result_commit=workspace.result_commit,
                )
                return existing
            if workspace.result_commit is not None:
                raise GitWorktreeError(
                    operation="worker_result_commit",
                    resource_id=worker_id,
                    message="Worker workspace has a result commit but no result record.",
                    recoverable=False,
                )

            execution = self._commit_runner.create_commit(
                worktree_root=worktree_root,
                expected_parent=workspace.base_commit,
                summary=summary,
            )
            result = self._result_repository.save(
                result=WorkerResultCommit(
                    run_id=run_id,
                    worker_id=worker_id,
                    base_commit=execution.parent_sha,
                    commit_sha=execution.commit_sha,
                    changed_paths=execution.changed_paths,
                    summary=summary,
                    created_at=datetime.now(UTC),
                )
            )
            self._commit_runner.activate_commit(
                worktree_root=worktree_root,
                expected_parent=result.base_commit,
                commit_sha=result.commit_sha,
            )
            self._worktree_repository.mark_result_commit(
                run_id=run_id,
                worker_id=worker_id,
                result_commit=result.commit_sha,
            )
            return result

    def read(self, run_id: str, worker_id: str) -> WorkerResultCommit | None:
        """Read. Args: run_id, worker_id. Returns: WorkerResultCommit | None."""
        return self._result_repository.read(run_id=run_id, worker_id=worker_id)

    def list_for_run(self, run_id: str) -> tuple[WorkerResultCommit, ...]:
        """List for run. Args: run_id. Returns: tuple[WorkerResultCommit, ...]."""
        return self._result_repository.list_for_run(run_id=run_id)

    def _reconcile_existing_result(
        self,
        result: WorkerResultCommit,
        worktree_root: Path,
        recorded_result_commit: str | None,
    ) -> None:
        """Reconcile existing result. Args: result, worktree_root, recorded_result_commit. Returns: None."""
        self._commit_runner.activate_commit(
            worktree_root=worktree_root,
            expected_parent=result.base_commit,
            commit_sha=result.commit_sha,
        )
        if recorded_result_commit != result.commit_sha:
            self._worktree_repository.mark_result_commit(
                run_id=result.run_id,
                worker_id=result.worker_id,
                result_commit=result.commit_sha,
            )

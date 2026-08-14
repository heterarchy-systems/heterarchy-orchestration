from pathlib import Path

from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.git.git_worktree_dto import RunGitSnapshot, WorkerGitWorktree
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import WorkerStatus
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.orchestration_identity import capability_matches
from chattocodex.persistence.git_worktree_repository import GitWorktreeRepository
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    WorkerStateRepository,
)


class WorkerGitWorktreeService:
    def __init__(
        self,
        *,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        worktree_repository: GitWorktreeRepository,
        command_runner: GitCommandRunner,
        lock_registry: EntityLockRegistry,
    ) -> None:
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._worktree_repository = worktree_repository
        self._command_runner = command_runner
        self._lock_registry = lock_registry

    def prepare(
        self,
        *,
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> WorkerGitWorktree:
        run = self._run_repository.read(run_id=run_id)
        worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
        if not capability_matches(
            handle=worker_handle,
            expected_digest=worker.worker_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="worker_workspace_prepare",
                resource_id=worker_id,
            )
        if worker.status is not WorkerStatus.ACTIVE:
            raise InvalidTransitionError(
                operation="worker_workspace_prepare",
                resource_id=worker_id,
                message="Worker must join the Run before preparing a write workspace.",
            )

        entity_key = f"git-workspace:{run_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            existing = self._worktree_repository.read_worker_worktree(
                run_id=run_id,
                worker_id=worker_id,
            )
            if existing is not None:
                self._verify_existing_worktree(worktree=existing)
                return existing

            project_root = Path(run.project_root).resolve()
            snapshot = self._get_or_create_run_snapshot(
                run_id=run_id,
                project_root=project_root,
            )
            worktree_root = self._worktree_repository.worktree_root(
                run_id=run_id,
                worker_id=worker_id,
            )
            if worktree_root.exists():
                self._recover_existing_path(
                    worktree_root=worktree_root,
                    base_commit=snapshot.base_commit,
                )
            else:
                self._command_runner.add_detached_worktree(
                    project_root=project_root,
                    worktree_root=worktree_root,
                    base_commit=snapshot.base_commit,
                )
            self._verify_worktree_head(
                worktree_root=worktree_root,
                expected_commit=snapshot.base_commit,
            )
            return self._worktree_repository.save_worker_worktree(
                run_id=run_id,
                worker_id=worker_id,
                project_root=project_root,
                worktree_root=worktree_root,
                base_commit=snapshot.base_commit,
            )

    def _get_or_create_run_snapshot(
        self,
        *,
        run_id: str,
        project_root: Path,
    ) -> RunGitSnapshot:
        snapshot = self._worktree_repository.read_run_snapshot(run_id=run_id)
        if snapshot is not None:
            if Path(snapshot.project_root).resolve() != project_root:
                raise GitWorktreeError(
                    operation="worker_workspace_prepare",
                    resource_id=run_id,
                    message="Persisted Git project root does not match the Run project root.",
                    recoverable=False,
                )
            return snapshot

        base_commit = self._command_runner.require_run_ready_repository(
            project_root=project_root,
            operation="worker_workspace_prepare",
        )
        return self._worktree_repository.create_run_snapshot(
            run_id=run_id,
            project_root=project_root,
            base_commit=base_commit,
        )

    def _recover_existing_path(
        self,
        *,
        worktree_root: Path,
        base_commit: str,
    ) -> None:
        self._verify_worktree_head(
            worktree_root=worktree_root,
            expected_commit=base_commit,
        )

    def _verify_existing_worktree(self, *, worktree: WorkerGitWorktree) -> None:
        self._verify_worktree_head(
            worktree_root=Path(worktree.worktree_root),
            expected_commit=worktree.result_commit or worktree.base_commit,
        )

    def _verify_worktree_head(
        self,
        *,
        worktree_root: Path,
        expected_commit: str,
    ) -> None:
        if not worktree_root.is_dir():
            raise GitWorktreeError(
                operation="worker_workspace_prepare",
                resource_id=str(worktree_root),
                message="Persisted Worker worktree directory is missing.",
                recoverable=True,
            )
        actual_commit = self._command_runner.head_commit(repository_root=worktree_root)
        if actual_commit != expected_commit:
            raise GitWorktreeError(
                operation="worker_workspace_prepare",
                resource_id=str(worktree_root),
                message=(
                    "Worker worktree HEAD no longer matches the Run base commit. "
                    "Do not silently reuse a drifted workspace."
                ),
                recoverable=False,
            )

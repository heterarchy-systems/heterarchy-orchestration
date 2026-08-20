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
from chattocodex.orchestrator_backend.repository.project_path_resolver import (
    ProjectPathResolver,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    ProjectPathDeniedError,
)
from chattocodex.orchestrator_backend.version_control.git_exceptions import (
    GitWorktreeError,
)


class WorkerWorkspaceAccessService:
    """Authorize and resolve paths inside one Worker isolated worktree."""

    def __init__(
        self,
        worker_repository: WorkerStateRepository,
        worktree_repository: GitWorktreeRepository,
        path_resolver: ProjectPathResolver,
    ) -> None:
        """Configure the Worker workspace access boundary.

        Args:
            worker_repository: Durable Worker state repository used for capability checks.
            worktree_repository: Repository that records prepared isolated worktrees.
            path_resolver: Canonical project-relative path resolver.

        Returns:
            None.
        """
        self._worker_repository = worker_repository
        self._worktree_repository = worktree_repository
        self._path_resolver = path_resolver

    def require_workspace(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        operation: str,
        require_mutable: bool,
    ) -> Path:
        """Return an authorized Worker worktree root for one operation.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated workspace is being accessed.
            worker_handle: Capability proving control of the Worker.
            operation: Stable operation name used in structured errors.
            require_mutable: Whether a committed result must make access fail.

        Returns:
            Canonical absolute path of the prepared isolated worktree.

        Raises:
            CapabilityDeniedError: If the Worker capability does not match.
            InvalidTransitionError: If the Worker is inactive or the worktree is immutable.
            GitWorktreeError: If no prepared worktree exists on disk.
        """
        worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
        if not capability_matches(
            handle=worker_handle,
            expected_digest=worker.worker_handle_digest,
        ):
            raise CapabilityDeniedError(operation=operation, resource_id=worker_id)
        if worker.status is not WorkerStatus.ACTIVE:
            raise InvalidTransitionError(
                operation=operation,
                resource_id=worker_id,
                message="Worker must join the Run before accessing its workspace.",
            )
        workspace = self._worktree_repository.read_worker_worktree(
            run_id=run_id,
            worker_id=worker_id,
        )
        if workspace is None:
            raise GitWorktreeError(
                operation=operation,
                resource_id=worker_id,
                message=(
                    "Worker workspace is not prepared. "
                    "Call worker_workspace_prepare first."
                ),
                recoverable=True,
            )
        if require_mutable and workspace.result_commit is not None:
            raise InvalidTransitionError(
                operation=operation,
                resource_id=worker_id,
                message="Worker result is already committed and the workspace is immutable.",
            )
        worktree_root = Path(workspace.worktree_root).resolve()
        if not worktree_root.is_dir():
            raise GitWorktreeError(
                operation=operation,
                resource_id=worker_id,
                message="Prepared Worker worktree directory is missing.",
                recoverable=True,
            )
        return worktree_root

    def resolve_path(
        self,
        worktree_root: Path,
        relative_path: str,
        operation: str,
    ) -> Path:
        """Resolve one path while blocking Git metadata and symlink traversal.

        Args:
            worktree_root: Authorized canonical Worker worktree root.
            relative_path: Relative path supplied by the Worker.
            operation: Stable operation name used in structured errors.

        Returns:
            Canonical path constrained to the Worker worktree.

        Raises:
            ProjectPathDeniedError: If the request targets Git metadata, traverses a
                symlink, is absolute, escapes the root, or contains ``..``.
        """
        requested = Path(relative_path or ".")
        if ".git" in requested.parts:
            raise ProjectPathDeniedError(
                operation=operation,
                relative_path=relative_path,
            )
        current = worktree_root
        for part in requested.parts:
            if part in {"", "."}:
                continue
            current = current / part
            if current.is_symlink():
                raise ProjectPathDeniedError(
                    operation=operation,
                    relative_path=relative_path,
                )
        return self._path_resolver.resolve(
            project_root=worktree_root,
            relative_path=relative_path,
            operation=operation,
        )

import os
import shutil
from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    ProjectPathDeniedError,
    UnsupportedRepositoryFileError,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_dto import (
    WorkerDirectoryMutationResult,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_workspace_access_service import (
    WorkerWorkspaceAccessService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class WorkerDirectoryMutationService:
    """Create, move, and delete directories inside authorized Worker worktrees."""

    def __init__(
        self,
        access_service: WorkerWorkspaceAccessService,
        lock_registry: EntityLockRegistry,
    ) -> None:
        """Configure Worker directory mutations.

        Args:
            access_service: Capability and path boundary for Worker worktrees.
            lock_registry: Cross-thread/process workspace mutation lock registry.

        Returns:
            None.
        """
        self._access_service = access_service
        self._lock_registry = lock_registry

    def create_directory(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        parents: bool,
    ) -> WorkerDirectoryMutationResult:
        """Create one directory inside a mutable Worker worktree.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative directory path to create.
            parents: Whether missing parent directories may also be created.

        Returns:
            Metadata for the created directory.

        Raises:
            ResourceAlreadyExistsError: If the target already exists.
            ResourceNotFoundError: If parents are missing and ``parents`` is false.
        """
        entity_key = _workspace_lock_key(run_id=run_id, worker_id=worker_id)
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._access_service.require_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                operation="worker_directory_create",
                require_mutable=True,
            )
            target = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=relative_path,
                operation="worker_directory_create",
            )
            _require_non_root_target(
                worktree_root=worktree_root,
                target=target,
                operation="worker_directory_create",
                relative_path=relative_path,
            )
            if target.exists():
                raise ResourceAlreadyExistsError(
                    operation="worker_directory_create",
                    resource_id=relative_path,
                )
            if not parents and not target.parent.is_dir():
                raise ResourceNotFoundError(
                    operation="worker_directory_create",
                    resource_id=target.parent.relative_to(worktree_root).as_posix(),
                )
            target.mkdir(parents=parents, exist_ok=False)
            _fsync_directory(path=target.parent)
            return WorkerDirectoryMutationResult(
                run_id=run_id,
                worker_id=worker_id,
                relative_path=_relative_path(worktree_root=worktree_root, path=target),
            )

    def move_directory(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        source_relative_path: str,
        destination_relative_path: str,
    ) -> WorkerDirectoryMutationResult:
        """Move or rename one directory inside a mutable Worker worktree.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Capability proving control of the Worker.
            source_relative_path: Existing worktree-relative source directory.
            destination_relative_path: Non-existing destination path.

        Returns:
            Metadata for the directory at its new path.

        Raises:
            ResourceNotFoundError: If the source is not a directory or destination parent is missing.
            ResourceAlreadyExistsError: If the destination already exists.
            ProjectPathDeniedError: If the move would place a directory inside itself.
        """
        entity_key = _workspace_lock_key(run_id=run_id, worker_id=worker_id)
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._access_service.require_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                operation="worker_directory_move",
                require_mutable=True,
            )
            source = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=source_relative_path,
                operation="worker_directory_move",
            )
            destination = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=destination_relative_path,
                operation="worker_directory_move",
            )
            _require_non_root_target(
                worktree_root=worktree_root,
                target=source,
                operation="worker_directory_move",
                relative_path=source_relative_path,
            )
            if not source.is_dir():
                raise ResourceNotFoundError(
                    operation="worker_directory_move",
                    resource_id=source_relative_path,
                )
            if destination.exists():
                raise ResourceAlreadyExistsError(
                    operation="worker_directory_move",
                    resource_id=destination_relative_path,
                )
            if not destination.parent.is_dir():
                raise ResourceNotFoundError(
                    operation="worker_directory_move",
                    resource_id=destination.parent.relative_to(
                        worktree_root
                    ).as_posix(),
                )
            if destination == source or destination.is_relative_to(source):
                raise ProjectPathDeniedError(
                    operation="worker_directory_move",
                    relative_path=destination_relative_path,
                )
            source_parent = source.parent
            source.rename(destination)
            _fsync_directory(path=source_parent)
            if destination.parent != source_parent:
                _fsync_directory(path=destination.parent)
            return WorkerDirectoryMutationResult(
                run_id=run_id,
                worker_id=worker_id,
                relative_path=_relative_path(
                    worktree_root=worktree_root,
                    path=destination,
                ),
            )

    def delete_directory(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        recursive: bool,
    ) -> WorkerDirectoryMutationResult:
        """Delete one directory, requiring explicit recursion for non-empty trees.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative directory path to delete.
            recursive: Whether a non-empty directory tree may be removed.

        Returns:
            Metadata describing the deleted relative path.

        Raises:
            ResourceNotFoundError: If the target is not a directory.
            UnsupportedRepositoryFileError: If a non-empty directory is deleted without recursion.
        """
        entity_key = _workspace_lock_key(run_id=run_id, worker_id=worker_id)
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._access_service.require_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                operation="worker_directory_delete",
                require_mutable=True,
            )
            target = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=relative_path,
                operation="worker_directory_delete",
            )
            _require_non_root_target(
                worktree_root=worktree_root,
                target=target,
                operation="worker_directory_delete",
                relative_path=relative_path,
            )
            if not target.is_dir():
                raise ResourceNotFoundError(
                    operation="worker_directory_delete",
                    resource_id=relative_path,
                )
            deleted_relative_path = _relative_path(
                worktree_root=worktree_root,
                path=target,
            )
            parent = target.parent
            if recursive:
                shutil.rmtree(target)
            else:
                try:
                    target.rmdir()
                except OSError as error:
                    raise UnsupportedRepositoryFileError(
                        operation="worker_directory_delete",
                        relative_path=relative_path,
                        message=(
                            "Directory is not empty. Retry with recursive=true only when "
                            "removing the complete subtree is intended."
                        ),
                    ) from error
            _fsync_directory(path=parent)
            return WorkerDirectoryMutationResult(
                run_id=run_id,
                worker_id=worker_id,
                relative_path=deleted_relative_path,
            )


def _workspace_lock_key(run_id: str, worker_id: str) -> str:
    """Build the stable mutation lock key for one Worker worktree.

    Args:
        run_id: Run that owns the Worker.
        worker_id: Worker whose worktree is locked.

    Returns:
        Stable lock registry key.
    """
    return f"worker-workspace:{run_id}:{worker_id}"


def _relative_path(worktree_root: Path, path: Path) -> str:
    """Render a canonical path relative to the Worker worktree.

    Args:
        worktree_root: Canonical Worker worktree root.
        path: Canonical path inside that root.

    Returns:
        POSIX-style relative path, using ``.`` for the root itself.
    """
    relative = path.relative_to(worktree_root)
    return relative.as_posix() if relative.parts else "."


def _require_non_root_target(
    worktree_root: Path,
    target: Path,
    operation: str,
    relative_path: str,
) -> None:
    """Reject mutations that target the Worker worktree root itself.

    Args:
        worktree_root: Canonical Worker worktree root.
        target: Resolved mutation target.
        operation: Stable operation name used in structured errors.
        relative_path: Original user-facing relative path.

    Returns:
        None.

    Raises:
        ProjectPathDeniedError: If the target is the worktree root.
    """
    if target == worktree_root:
        raise ProjectPathDeniedError(
            operation=operation,
            relative_path=relative_path,
        )


def _fsync_directory(path: Path) -> None:
    """Synchronize one directory entry mutation to durable storage.

    Args:
        path: Directory whose entries changed.

    Returns:
        None.
    """
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)

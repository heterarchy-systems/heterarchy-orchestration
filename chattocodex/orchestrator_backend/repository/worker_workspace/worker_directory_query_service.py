from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.repository.filesystem_entry_metadata import (
    read_filesystem_entry_metadata,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_dto import (
    WorkerDirectoryEntry,
    WorkerDirectoryListing,
    WorkerWorkspacePathInfo,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_workspace_access_service import (
    WorkerWorkspaceAccessService,
)
from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind


class WorkerDirectoryQueryService:
    """Inspect directories and paths inside authorized Worker worktrees."""

    def __init__(self, access_service: WorkerWorkspaceAccessService) -> None:
        """Configure read-only Worker directory queries.

        Args:
            access_service: Capability and path boundary for Worker worktrees.

        Returns:
            None.
        """
        self._access_service = access_service

    def list_directory(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
    ) -> WorkerDirectoryListing:
        """List one directory without exposing Git metadata.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is inspected.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative directory path, including ``.`` for root.

        Returns:
            Stable case-insensitive sorted directory listing.

        Raises:
            ResourceNotFoundError: If the requested path is not a directory.
        """
        worktree_root = self._access_service.require_workspace(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            operation="worker_directory_list",
            require_mutable=False,
        )
        directory = self._access_service.resolve_path(
            worktree_root=worktree_root,
            relative_path=relative_path,
            operation="worker_directory_list",
        )
        if not directory.is_dir():
            raise ResourceNotFoundError(
                operation="worker_directory_list",
                resource_id=relative_path,
            )
        entries = tuple(
            sorted(
                (
                    _entry_from_path(worktree_root=worktree_root, path=path)
                    for path in directory.iterdir()
                    if path.name != ".git"
                ),
                key=lambda entry: entry.name.casefold(),
            )
        )
        return WorkerDirectoryListing(
            run_id=run_id,
            worker_id=worker_id,
            relative_path=_relative_path(worktree_root=worktree_root, path=directory),
            entries=entries,
        )

    def stat_path(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
    ) -> WorkerWorkspacePathInfo:
        """Return strict metadata for one authorized worktree path.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is inspected.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative path to inspect.

        Returns:
            Path kind and optional file size.

        Raises:
            ResourceNotFoundError: If the path does not exist.
        """
        worktree_root = self._access_service.require_workspace(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            operation="worker_path_stat",
            require_mutable=False,
        )
        target = self._access_service.resolve_path(
            worktree_root=worktree_root,
            relative_path=relative_path,
            operation="worker_path_stat",
        )
        metadata = read_filesystem_entry_metadata(path=target)
        if metadata is None:
            raise ResourceNotFoundError(
                operation="worker_path_stat",
                resource_id=relative_path,
            )
        return WorkerWorkspacePathInfo(
            run_id=run_id,
            worker_id=worker_id,
            relative_path=_relative_path(worktree_root=worktree_root, path=target),
            kind=metadata.kind,
            size_bytes=metadata.size_bytes,
        )


def _entry_from_path(worktree_root: Path, path: Path) -> WorkerDirectoryEntry:
    """Map one filesystem entry into an internal Worker directory DTO.

    Args:
        worktree_root: Canonical Worker worktree root.
        path: Direct child entry to describe.

    Returns:
        Strict immutable entry metadata.
    """
    metadata = read_filesystem_entry_metadata(path=path)
    kind = metadata.kind if metadata is not None else RepositoryEntryKind.OTHER
    size_bytes = metadata.size_bytes if metadata is not None else None
    return WorkerDirectoryEntry(
        name=path.name,
        relative_path=_relative_path(worktree_root=worktree_root, path=path),
        kind=kind,
        size_bytes=size_bytes,
    )


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

import hashlib
import os
import stat
from pathlib import Path
from uuid import uuid4

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    UnsupportedRepositoryFileError,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.repository_write_dto import (
    WorkerFileMutationResult,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.repository_write_exceptions import (
    ContentHashConflictError,
    RepositoryWriteError,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_workspace_access_service import (
    WorkerWorkspaceAccessService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_MAX_WRITE_BYTES = 1_048_576


class WorkerFileMutationService:
    """Create and replace text files inside authorized Worker worktrees."""

    def __init__(
        self,
        access_service: WorkerWorkspaceAccessService,
        lock_registry: EntityLockRegistry,
    ) -> None:
        """Configure Worker file mutations.

        Args:
            access_service: Capability and path boundary for Worker worktrees.
            lock_registry: Cross-thread/process workspace mutation lock registry.

        Returns:
            None.
        """
        self._access_service = access_service
        self._lock_registry = lock_registry

    def create_file(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        content: str,
    ) -> WorkerFileMutationResult:
        """Atomically create one UTF-8 text file in a Worker worktree.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative target file path.
            content: UTF-8 text content to persist.

        Returns:
            Metadata for the newly created file.

        Raises:
            ResourceAlreadyExistsError: If the target already exists.
            UnsupportedRepositoryFileError: If the target is unsupported or too large.
            RepositoryWriteError: If the atomic write fails.
        """
        entity_key = _workspace_lock_key(run_id=run_id, worker_id=worker_id)
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._access_service.require_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                operation="worker_file_create",
                require_mutable=True,
            )
            target = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=relative_path,
                operation="worker_file_create",
            )
            if target.exists():
                raise ResourceAlreadyExistsError(
                    operation="worker_file_create",
                    resource_id=relative_path,
                )
            payload = _encode_content(content=content, relative_path=relative_path)
            _atomic_write(path=target, payload=payload, mode=0o644)
            return _build_result(
                run_id=run_id,
                worker_id=worker_id,
                worktree_root=worktree_root,
                target=target,
            )

    def replace_file(
        self,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        expected_sha256: str,
        content: str,
    ) -> WorkerFileMutationResult:
        """Atomically replace one text file when its SHA-256 precondition matches.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Capability proving control of the Worker.
            relative_path: Worktree-relative target file path.
            expected_sha256: Required digest of the currently stored file.
            content: Replacement UTF-8 text content.

        Returns:
            Metadata for the replaced file.

        Raises:
            ResourceNotFoundError: If the target is not a regular file.
            ContentHashConflictError: If the optimistic concurrency digest is stale.
            UnsupportedRepositoryFileError: If the target is unsupported or too large.
            RepositoryWriteError: If the atomic write fails.
        """
        entity_key = _workspace_lock_key(run_id=run_id, worker_id=worker_id)
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._access_service.require_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                operation="worker_file_replace",
                require_mutable=True,
            )
            target = self._access_service.resolve_path(
                worktree_root=worktree_root,
                relative_path=relative_path,
                operation="worker_file_replace",
            )
            if not target.is_file():
                raise ResourceNotFoundError(
                    operation="worker_file_replace",
                    resource_id=relative_path,
                )
            current_payload = _read_bounded_file(
                path=target,
                relative_path=relative_path,
            )
            current_sha256 = _sha256(payload=current_payload)
            if current_sha256 != expected_sha256:
                raise ContentHashConflictError(
                    relative_path=relative_path,
                    expected_sha256=expected_sha256,
                    current_sha256=current_sha256,
                )
            payload = _encode_content(content=content, relative_path=relative_path)
            mode = stat.S_IMODE(target.stat().st_mode)
            _atomic_write(path=target, payload=payload, mode=mode)
            return _build_result(
                run_id=run_id,
                worker_id=worker_id,
                worktree_root=worktree_root,
                target=target,
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


def _encode_content(content: str, relative_path: str) -> bytes:
    """Encode bounded UTF-8 text content for persistence.

    Args:
        content: Text content supplied by the Worker.
        relative_path: Target path used in structured error reporting.

    Returns:
        UTF-8 encoded payload.

    Raises:
        UnsupportedRepositoryFileError: If NUL bytes are present or size exceeds 1 MiB.
    """
    if "\x00" in content:
        raise UnsupportedRepositoryFileError(
            operation="worker_file_mutation",
            relative_path=relative_path,
            message="NUL bytes are not allowed in text file mutations.",
        )
    payload = content.encode("utf-8")
    if len(payload) > _MAX_WRITE_BYTES:
        raise UnsupportedRepositoryFileError(
            operation="worker_file_mutation",
            relative_path=relative_path,
            message="Worker file mutation exceeds the 1 MiB limit.",
        )
    return payload


def _read_bounded_file(path: Path, relative_path: str) -> bytes:
    """Read one file while enforcing the replacement size limit.

    Args:
        path: Canonical file path to read.
        relative_path: User-facing relative path used in structured errors.

    Returns:
        File payload bytes.

    Raises:
        UnsupportedRepositoryFileError: If the file exceeds 1 MiB.
        RepositoryWriteError: If the file cannot be read.
    """
    try:
        size_bytes = path.stat().st_size
        if size_bytes > _MAX_WRITE_BYTES:
            raise UnsupportedRepositoryFileError(
                operation="worker_file_replace",
                relative_path=relative_path,
                message="Existing file exceeds the 1 MiB replace limit.",
            )
        return path.read_bytes()
    except OSError as error:
        raise RepositoryWriteError(
            operation="worker_file_replace",
            relative_path=relative_path,
            message="Existing file could not be read for replacement precondition check.",
        ) from error


def _atomic_write(path: Path, payload: bytes, mode: int) -> None:
    """Persist bytes using fsync plus atomic rename semantics.

    Args:
        path: Destination file path.
        payload: Bytes to persist.
        mode: POSIX permissions for the temporary file.

    Returns:
        None.

    Raises:
        RepositoryWriteError: If writing, syncing, or replacement fails.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    file_descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        mode,
    )
    try:
        with os.fdopen(file_descriptor, "wb") as file_handle:
            file_handle.write(payload)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path=path.parent)
    except OSError as error:
        raise RepositoryWriteError(
            operation="worker_file_mutation",
            relative_path=path.name,
            message="Worker file mutation could not be persisted atomically.",
        ) from error
    finally:
        temporary_path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    """Synchronize a directory entry update to durable storage.

    Args:
        path: Directory containing an atomically replaced file.

    Returns:
        None.
    """
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _build_result(
    run_id: str,
    worker_id: str,
    worktree_root: Path,
    target: Path,
) -> WorkerFileMutationResult:
    """Build a strict internal result for a persisted Worker file mutation.

    Args:
        run_id: Run that owns the Worker.
        worker_id: Worker that mutated the file.
        worktree_root: Canonical Worker worktree root.
        target: Canonical target file path.

    Returns:
        Mutation metadata including relative path, digest, and size.
    """
    payload = _read_bounded_file(path=target, relative_path=str(target))
    return WorkerFileMutationResult(
        run_id=run_id,
        worker_id=worker_id,
        relative_path=target.relative_to(worktree_root).as_posix(),
        sha256=_sha256(payload=payload),
        size_bytes=len(payload),
    )


def _sha256(payload: bytes) -> str:
    """Return the lowercase SHA-256 digest for bytes.

    Args:
        payload: Bytes to hash.

    Returns:
        Sixty-four-character lowercase hexadecimal digest.
    """
    return hashlib.sha256(payload).hexdigest()

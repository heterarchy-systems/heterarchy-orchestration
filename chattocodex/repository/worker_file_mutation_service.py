import hashlib
import os
import stat
from pathlib import Path
from uuid import uuid4

from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import WorkerStatus
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from chattocodex.orchestration.orchestration_identity import capability_matches
from chattocodex.persistence.git_worktree_repository import GitWorktreeRepository
from chattocodex.persistence.orchestration_state_repository import WorkerStateRepository
from chattocodex.repository.project_path_resolver import ProjectPathResolver
from chattocodex.repository.repository_exceptions import (
    ProjectPathDeniedError,
    UnsupportedRepositoryFileError,
)
from chattocodex.repository.repository_write_dto import WorkerFileMutationResult
from chattocodex.repository.repository_write_exceptions import (
    ContentHashConflictError,
    RepositoryWriteError,
)

_MAX_WRITE_BYTES = 1_048_576


class WorkerFileMutationService:
    def __init__(
        self,
        *,
        worker_repository: WorkerStateRepository,
        worktree_repository: GitWorktreeRepository,
        path_resolver: ProjectPathResolver,
        lock_registry: EntityLockRegistry,
    ) -> None:
        self._worker_repository = worker_repository
        self._worktree_repository = worktree_repository
        self._path_resolver = path_resolver
        self._lock_registry = lock_registry

    def create_file(
        self,
        *,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        content: str,
    ) -> WorkerFileMutationResult:
        entity_key = f"worker-workspace:{run_id}:{worker_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._require_worker_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
            )
            target = _resolve_write_target(
                resolver=self._path_resolver,
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
        *,
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        expected_sha256: str,
        content: str,
    ) -> WorkerFileMutationResult:
        entity_key = f"worker-workspace:{run_id}:{worker_id}"
        with self._lock_registry.hold(entity_key=entity_key):
            worktree_root = self._require_worker_workspace(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
            )
            target = _resolve_write_target(
                resolver=self._path_resolver,
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

    def _require_worker_workspace(
        self,
        *,
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> Path:
        worker = self._worker_repository.read(run_id=run_id, worker_id=worker_id)
        if not capability_matches(
            handle=worker_handle,
            expected_digest=worker.worker_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="worker_file_mutation",
                resource_id=worker_id,
            )
        if worker.status is not WorkerStatus.ACTIVE:
            raise InvalidTransitionError(
                operation="worker_file_mutation",
                resource_id=worker_id,
                message="Worker must join the Run before mutating files.",
            )
        workspace = self._worktree_repository.read_worker_worktree(
            run_id=run_id,
            worker_id=worker_id,
        )
        if workspace is None:
            raise GitWorktreeError(
                operation="worker_file_mutation",
                resource_id=worker_id,
                message=(
                    "Worker write workspace is not prepared. "
                    "Call worker_workspace_prepare first."
                ),
                recoverable=True,
            )
        if workspace.result_commit is not None:
            raise InvalidTransitionError(
                operation="worker_file_mutation",
                resource_id=worker_id,
                message="Worker result is already committed and the workspace is immutable.",
            )
        worktree_root = Path(workspace.worktree_root).resolve()
        if not worktree_root.is_dir():
            raise GitWorktreeError(
                operation="worker_file_mutation",
                resource_id=worker_id,
                message="Prepared Worker worktree directory is missing.",
                recoverable=True,
            )
        return worktree_root


def _resolve_write_target(
    *,
    resolver: ProjectPathResolver,
    worktree_root: Path,
    relative_path: str,
    operation: str,
) -> Path:
    requested = Path(relative_path)
    if requested.parts and requested.parts[0] == ".git":
        raise ProjectPathDeniedError(operation=operation, relative_path=relative_path)
    current = worktree_root
    for part in requested.parts:
        current = current / part
        if current.is_symlink():
            raise ProjectPathDeniedError(
                operation=operation, relative_path=relative_path
            )
    target = resolver.resolve(
        project_root=worktree_root,
        relative_path=relative_path,
        operation=operation,
    )
    if target.exists() and target.is_dir():
        raise UnsupportedRepositoryFileError(
            operation=operation,
            relative_path=relative_path,
            message="Directory mutation is not supported by Worker file tools.",
        )
    return target


def _encode_content(*, content: str, relative_path: str) -> bytes:
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


def _read_bounded_file(*, path: Path, relative_path: str) -> bytes:
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


def _atomic_write(*, path: Path, payload: bytes, mode: int) -> None:
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


def _fsync_directory(*, path: Path) -> None:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _build_result(
    *,
    run_id: str,
    worker_id: str,
    worktree_root: Path,
    target: Path,
) -> WorkerFileMutationResult:
    payload = _read_bounded_file(path=target, relative_path=str(target))
    return WorkerFileMutationResult(
        run_id=run_id,
        worker_id=worker_id,
        relative_path=target.relative_to(worktree_root).as_posix(),
        sha256=_sha256(payload=payload),
        size_bytes=len(payload),
    )


def _sha256(*, payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()

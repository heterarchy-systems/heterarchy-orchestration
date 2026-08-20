from typing import Annotated, Literal

from chattocodex.shared.contracts.orchestration_schemas import (
    RunId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.contracts.repository.repository_write_schemas import (
    RepositoryWritePath,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind


class WorkerWorkspaceBoundaryModel(StrictSchemaModel):
    """Base contract for Worker filesystem MCP boundaries."""


class WorkerDirectoryListRequest(WorkerWorkspaceBoundaryModel):
    """Validate a Worker directory listing request."""

    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath = "."


class WorkerPathStatRequest(WorkerWorkspaceBoundaryModel):
    """Validate a Worker path metadata request."""

    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath


class WorkerDirectoryCreateRequest(WorkerWorkspaceBoundaryModel):
    """Validate a Worker directory creation request."""

    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath
    parents: bool = False


class WorkerDirectoryMoveRequest(WorkerWorkspaceBoundaryModel):
    """Validate a Worker directory move or rename request."""

    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    source_relative_path: RepositoryWritePath
    destination_relative_path: RepositoryWritePath


class WorkerDirectoryDeleteRequest(WorkerWorkspaceBoundaryModel):
    """Validate a Worker directory deletion request."""

    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath
    recursive: bool = False


class WorkerDirectoryEntryView(WorkerWorkspaceBoundaryModel):
    """Describe one entry returned from a Worker directory listing."""

    name: RepositoryWritePath
    relative_path: RepositoryWritePath
    kind: RepositoryEntryKind
    size_bytes: Annotated[
        int | None,
        described_field(
            description="File size in bytes when the entry is a regular file.",
            ge=0,
        ),
    ] = None


class WorkerDirectoryListSuccess(WorkerWorkspaceBoundaryModel):
    """Return one successful Worker directory listing."""

    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    relative_path: RepositoryWritePath
    entries: tuple[WorkerDirectoryEntryView, ...]


class WorkerPathStatSuccess(WorkerWorkspaceBoundaryModel):
    """Return metadata for one Worker worktree path."""

    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    relative_path: RepositoryWritePath
    kind: RepositoryEntryKind
    size_bytes: Annotated[
        int | None,
        described_field(
            description="File size in bytes when the path is a regular file.",
            ge=0,
        ),
    ] = None


class WorkerDirectoryMutationSuccess(WorkerWorkspaceBoundaryModel):
    """Return one successful Worker directory mutation."""

    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    relative_path: RepositoryWritePath

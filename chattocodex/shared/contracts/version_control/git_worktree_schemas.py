from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import StringConstraints

from chattocodex.shared.contracts.orchestration_schemas import (
    RunId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.workspace.source_control_types import GitCommit


class GitWorktreeBoundaryModel(StrictSchemaModel):
    pass


class RunGitSnapshotRecord(GitWorktreeBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    project_root: Annotated[
        str,
        StringConstraints(strict=True, min_length=1, max_length=4096),
        described_field(description="Project root for this run git snapshot record."),
    ]
    base_commit: GitCommit
    created_at: datetime


class WorkerGitWorktreeRecord(GitWorktreeBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    worker_id: WorkerId
    project_root: Annotated[
        str,
        StringConstraints(strict=True, min_length=1, max_length=4096),
        described_field(
            description="Project root for this worker git worktree record."
        ),
    ]
    worktree_root: Annotated[
        str,
        StringConstraints(strict=True, min_length=1, max_length=4096),
        described_field(
            description="Worktree root for this worker git worktree record."
        ),
    ]
    base_commit: GitCommit
    result_commit: GitCommit | None = None
    prepared_at: datetime


class WorkerWorkspacePrepareRequest(GitWorktreeBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle


class WorkerWorkspacePrepareSuccess(GitWorktreeBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    worktree_root: Path
    base_commit: GitCommit

from datetime import datetime
from typing import Annotated, Literal

from pydantic import StringConstraints

from chattocodex.shared.contracts.orchestration_schemas import (
    RunId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel
from chattocodex.shared.contracts.types.workspace.source_control_types import (
    ChangedPath,
)
from chattocodex.shared.contracts.version_control.git_worktree_schemas import GitCommit

CommitSummary = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=200),
]


class GitResultBoundaryModel(StrictSchemaModel):
    pass


class WorkerResultCommitRecord(GitResultBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    worker_id: WorkerId
    base_commit: GitCommit
    commit_sha: GitCommit
    changed_paths: tuple[ChangedPath, ...]
    summary: CommitSummary
    created_at: datetime


class WorkerResultCommitRequest(GitResultBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    summary: CommitSummary


class WorkerResultCommitSuccess(GitResultBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    base_commit: GitCommit
    commit_sha: GitCommit
    changed_paths: tuple[ChangedPath, ...]
    summary: CommitSummary
    created_at: datetime


class WorkerResultReadRequest(GitResultBoundaryModel):
    run_id: RunId
    worker_id: WorkerId


class WorkerResultReadSuccess(GitResultBoundaryModel):
    ok: Literal[True] = True
    result: WorkerResultCommitSuccess | None


class RunResultsRequest(GitResultBoundaryModel):
    run_id: RunId


class RunResultsSuccess(GitResultBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    results: tuple[WorkerResultCommitSuccess, ...]

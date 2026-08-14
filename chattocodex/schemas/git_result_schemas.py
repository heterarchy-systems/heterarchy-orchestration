from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints

from chattocodex.schemas.git_worktree_schemas import GitCommit
from chattocodex.schemas.orchestration_schemas import RunId, WorkerHandle, WorkerId

CommitSummary = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
]
ChangedPath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
]


class GitResultBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


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

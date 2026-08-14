from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from chattocodex.schemas.orchestration_schemas import (
    RunId,
    WorkerHandle,
    WorkerId,
)

GitCommit = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^[0-9a-f]{40,64}$"),
]


class GitWorktreeBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class RunGitSnapshotRecord(GitWorktreeBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    project_root: str = Field(min_length=1, max_length=4096)
    base_commit: GitCommit
    created_at: datetime


class WorkerGitWorktreeRecord(GitWorktreeBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    worker_id: WorkerId
    project_root: str = Field(min_length=1, max_length=4096)
    worktree_root: str = Field(min_length=1, max_length=4096)
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

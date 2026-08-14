from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from chattocodex.schemas.git_worktree_schemas import GitCommit
from chattocodex.schemas.orchestration_schemas import RunHandle, RunId, WorkerId

ChangedPath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
]


class IntegrationBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class IntegrationPreviewRequest(IntegrationBoundaryModel):
    run_id: RunId


class IntegrationApplyRequest(IntegrationBoundaryModel):
    run_id: RunId
    run_handle: RunHandle


class IntegrationConflictView(IntegrationBoundaryModel):
    relative_path: ChangedPath
    worker_ids: tuple[WorkerId, ...]
    commit_shas: tuple[GitCommit, ...]


class IntegrationPreviewSuccess(IntegrationBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    base_commit: GitCommit | None
    result_count: int = Field(ge=0)
    ready_to_integrate: bool
    conflicts: tuple[IntegrationConflictView, ...]


class IntegrationApplySuccess(IntegrationBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    base_commit: GitCommit
    integration_commit: GitCommit
    result_count: int = Field(ge=1)
    changed_paths: tuple[ChangedPath, ...]

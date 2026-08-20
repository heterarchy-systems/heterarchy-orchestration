from typing import Annotated, Literal

from chattocodex.shared.contracts.orchestration_schemas import (
    RunHandle,
    RunId,
    WorkerId,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.workspace.source_control_types import (
    ChangedPath,
)
from chattocodex.shared.contracts.version_control.git_worktree_schemas import GitCommit


class IntegrationBoundaryModel(StrictSchemaModel):
    pass


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
    result_count: Annotated[
        int,
        described_field(
            description="Number of result entries represented by this integration preview success.",
            ge=0,
        ),
    ]
    ready_to_integrate: bool
    conflicts: tuple[IntegrationConflictView, ...]


class IntegrationApplySuccess(IntegrationBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    base_commit: GitCommit
    integration_commit: GitCommit
    result_count: Annotated[
        int,
        described_field(
            description="Number of result entries represented by this integration apply success.",
            ge=1,
        ),
    ]
    changed_paths: tuple[ChangedPath, ...]

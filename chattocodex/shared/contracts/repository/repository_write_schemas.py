from typing import Annotated, Literal

from pydantic import StringConstraints

from chattocodex.shared.contracts.orchestration_schemas import (
    RunId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field

RepositoryWritePath = Annotated[
    str,
    StringConstraints(
        strict=True, strip_whitespace=True, min_length=1, max_length=2048
    ),
]
Sha256Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]
FileContent = Annotated[str, StringConstraints(strict=True, max_length=1_000_000)]


class RepositoryWriteBoundaryModel(StrictSchemaModel):
    pass


class WorkerFileCreateRequest(RepositoryWriteBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath
    content: FileContent


class WorkerFileReplaceRequest(RepositoryWriteBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RepositoryWritePath
    expected_sha256: Sha256Digest
    content: FileContent


class WorkerFileMutationSuccess(RepositoryWriteBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    relative_path: RepositoryWritePath
    sha256: Sha256Digest
    size_bytes: Annotated[
        int,
        described_field(
            description="Size bytes for this worker file mutation success.",
            ge=0,
            le=1048576,
        ),
    ]

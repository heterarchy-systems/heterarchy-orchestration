from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from chattocodex.schemas.orchestration_schemas import RunId, WorkerHandle, WorkerId

RelativePath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2048),
]
Sha256Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
FileContent = Annotated[str, StringConstraints(max_length=1_000_000)]


class RepositoryWriteBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class WorkerFileCreateRequest(RepositoryWriteBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RelativePath
    content: FileContent


class WorkerFileReplaceRequest(RepositoryWriteBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    relative_path: RelativePath
    expected_sha256: Sha256Digest
    content: FileContent


class WorkerFileMutationSuccess(RepositoryWriteBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    worker_id: WorkerId
    relative_path: RelativePath
    sha256: Sha256Digest
    size_bytes: int = Field(ge=0, le=1_048_576)

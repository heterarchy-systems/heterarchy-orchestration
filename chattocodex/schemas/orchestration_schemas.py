from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from chattocodex.orchestration.orchestration_dto import (
    RunStatus,
    TaskStatus,
    WorkerStatus,
)
from chattocodex.orchestration.orchestration_exceptions import OrchestrationErrorCode
from chattocodex.schemas.project_schemas import ProjectId, ProjectName, ProjectRoot

RunId = Annotated[str, StringConstraints(pattern=r"^run_[0-9a-f]{24}$")]
WorkerId = Annotated[str, StringConstraints(pattern=r"^worker_[0-9a-f]{24}$")]
TaskId = Annotated[str, StringConstraints(pattern=r"^task_[0-9a-f]{24}$")]
RunHandle = Annotated[
    str,
    StringConstraints(pattern=r"^rnh_[A-Za-z0-9_-]{32,128}$"),
]
WorkerHandle = Annotated[
    str,
    StringConstraints(pattern=r"^wkh_[A-Za-z0-9_-]{32,128}$"),
]
CapabilityDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Revision = Annotated[int, Field(ge=0)]
GoalText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=10_000),
]
ObjectiveText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=10_000),
]
SummaryText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=20_000),
]
ResultText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=50_000),
]
RoleText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=120),
]


class BoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class TimestampedRecord(BoundaryModel):
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value


class RunRecord(TimestampedRecord):
    schema_version: Literal[1]
    run_id: RunId
    goal: GoalText
    project_id: ProjectId
    project_name: ProjectName
    project_root: ProjectRoot
    run_handle_digest: CapabilityDigest
    status: RunStatus
    revision: Revision
    requested_parallelism: int = Field(default=1, ge=1, le=32)


class WorkerRecord(TimestampedRecord):
    schema_version: Literal[1]
    run_id: RunId
    worker_id: WorkerId
    worker_handle_digest: CapabilityDigest
    status: WorkerStatus
    role: RoleText | None
    revision: Revision


class TaskRecord(TimestampedRecord):
    schema_version: Literal[1]
    run_id: RunId
    task_id: TaskId
    assigned_worker_id: WorkerId
    objective: ObjectiveText
    status: TaskStatus
    revision: Revision
    progress_summary: SummaryText | None
    result: ResultText | None


class RunCreateRequest(BoundaryModel):
    goal: GoalText
    project_id: ProjectId
    requested_parallelism: int = Field(default=1, ge=1, le=32)


class WorkerContextRequest(BoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle


class TaskUpdateRequest(BoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    task_id: TaskId
    expected_revision: Revision
    status: TaskStatus
    progress_summary: SummaryText
    result: ResultText | None = None


class RunView(BoundaryModel):
    run_id: RunId
    goal: GoalText
    project_id: ProjectId
    project_name: ProjectName
    project_root: ProjectRoot
    status: RunStatus
    revision: Revision
    requested_parallelism: int
    created_at: datetime
    updated_at: datetime


class WorkerView(BoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    status: WorkerStatus
    role: RoleText | None
    revision: Revision
    created_at: datetime
    updated_at: datetime


class TaskView(BoundaryModel):
    run_id: RunId
    task_id: TaskId
    assigned_worker_id: WorkerId
    objective: ObjectiveText
    status: TaskStatus
    revision: Revision
    progress_summary: SummaryText | None
    result: ResultText | None
    created_at: datetime
    updated_at: datetime


class RunCreateSuccess(BoundaryModel):
    ok: Literal[True] = True
    run: RunView
    run_handle: RunHandle


class WorkerContextSuccess(BoundaryModel):
    ok: Literal[True] = True
    run: RunView
    worker: WorkerView
    tasks: tuple[TaskView, ...]


class TaskUpdateSuccess(BoundaryModel):
    ok: Literal[True] = True
    task: TaskView


class ToolError(BoundaryModel):
    ok: Literal[False] = False
    error_code: OrchestrationErrorCode
    message: str
    operation: str
    resource_id: str | None
    recoverable: bool
    current_revision: int | None = None

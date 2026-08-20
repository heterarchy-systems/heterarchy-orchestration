from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator

from chattocodex.shared.contracts.project.project_schemas import (
    ProjectId,
    ProjectName,
    ProjectRoot,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.coordination.orchestration_types import (
    CapabilityDigest,
    GoalText,
    ObjectiveText,
    ResultText,
    Revision,
    RoleText,
    RunHandle,
    RunId,
    SummaryText,
    TaskId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.element_collect.orchestration_enums import (
    OrchestrationErrorCode,
    RunStatus,
    TaskStatus,
    WorkerStatus,
)


class BoundaryModel(StrictSchemaModel):
    pass


class TimestampedRecord(BoundaryModel):
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
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
    requested_parallelism: Annotated[
        int,
        described_field(
            description="Requested parallelism for this run record.", ge=1, le=32
        ),
    ] = 1


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
    requested_parallelism: Annotated[
        int,
        described_field(
            description="Requested parallelism for this run create request.",
            ge=1,
            le=32,
        ),
    ] = 1


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

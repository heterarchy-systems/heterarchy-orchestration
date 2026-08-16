from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from chattocodex.broker.broker_dto import BrokerTaskStatus, WorkerSlotStatus
from chattocodex.schemas.orchestration_schemas import (
    ObjectiveText,
    Revision,
    RoleText,
    RunId,
    RunView,
    TaskView,
    WorkerHandle,
    WorkerView,
)
from chattocodex.schemas.project_schemas import ProjectId

BrokerTaskId = Annotated[str, StringConstraints(pattern=r"^btask_[0-9a-f]{24}$")]
WorkerSlotId = Annotated[str, StringConstraints(pattern=r"^slot_[0-9a-f]{24}$")]
WorkerSlotHandle = Annotated[
    str,
    StringConstraints(pattern=r"^slh_[A-Za-z0-9_-]{32,128}$"),
]
CapabilityDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SlotLabel = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=120),
]
WaitSeconds = Annotated[float, Field(ge=0, le=30)]


class BrokerBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class BrokerTimestampedRecord(BrokerBoundaryModel):
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value


class BrokerTaskRecord(BrokerTimestampedRecord):
    schema_version: Literal[1] = 1
    broker_task_id: BrokerTaskId
    run_id: RunId
    objective: ObjectiveText
    role: RoleText | None
    depends_on: tuple[BrokerTaskId, ...] = ()
    status: BrokerTaskStatus
    assigned_slot_id: WorkerSlotId | None
    assigned_worker_id: str | None
    assigned_task_id: str | None
    revision: Revision

    @model_validator(mode="after")
    def validate_assignment_shape(self) -> BrokerTaskRecord:
        assignment = (
            self.assigned_slot_id,
            self.assigned_worker_id,
            self.assigned_task_id,
        )
        if self.status is BrokerTaskStatus.QUEUED and any(
            value is not None for value in assignment
        ):
            raise ValueError("queued broker tasks cannot retain an assignment")
        if self.status is BrokerTaskStatus.CLAIMED and any(
            value is None for value in assignment
        ):
            raise ValueError("claimed broker tasks require a complete assignment")
        return self


class WorkerSlotRecord(BrokerTimestampedRecord):
    schema_version: Literal[1] = 1
    slot_id: WorkerSlotId
    project_id: ProjectId
    slot_handle_digest: CapabilityDigest
    label: SlotLabel | None
    status: WorkerSlotStatus
    current_broker_task_id: BrokerTaskId | None
    current_run_id: RunId | None
    current_worker_id: str | None
    current_task_id: str | None
    revision: Revision

    @model_validator(mode="after")
    def validate_assignment_shape(self) -> WorkerSlotRecord:
        assignment = (
            self.current_broker_task_id,
            self.current_run_id,
            self.current_worker_id,
            self.current_task_id,
        )
        if self.status is WorkerSlotStatus.IDLE and any(
            value is not None for value in assignment
        ):
            raise ValueError("idle worker slots cannot retain an assignment")
        if self.status is WorkerSlotStatus.BUSY and any(
            value is None for value in assignment
        ):
            raise ValueError("busy worker slots require a complete assignment")
        return self


class BrokerTaskView(BrokerBoundaryModel):
    broker_task_id: BrokerTaskId
    run_id: RunId
    objective: ObjectiveText
    role: RoleText | None
    depends_on: tuple[BrokerTaskId, ...]
    status: BrokerTaskStatus
    assigned_slot_id: WorkerSlotId | None
    assigned_worker_id: str | None
    assigned_task_id: str | None
    revision: Revision
    created_at: datetime
    updated_at: datetime


class WorkerSlotView(BrokerBoundaryModel):
    slot_id: WorkerSlotId
    project_id: ProjectId
    label: SlotLabel | None
    status: WorkerSlotStatus
    current_broker_task_id: BrokerTaskId | None
    current_run_id: RunId | None
    current_worker_id: str | None
    current_task_id: str | None
    revision: Revision
    created_at: datetime
    updated_at: datetime


class WorkerAssignmentView(BrokerBoundaryModel):
    broker_task: BrokerTaskView
    slot: WorkerSlotView
    run: RunView
    worker: WorkerView
    task: TaskView
    worker_handle: WorkerHandle


class WorkerAssignmentFinishSuccess(BrokerBoundaryModel):
    ok: Literal[True] = True
    broker_task: BrokerTaskView
    slot: WorkerSlotView
    task: TaskView


class BrokerRunStatusSuccess(BrokerBoundaryModel):
    ok: Literal[True] = True
    run: RunView
    broker_tasks: tuple[BrokerTaskView, ...]
    slots: tuple[WorkerSlotView, ...]
    ready_count: int
    queued_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int
    is_complete: bool

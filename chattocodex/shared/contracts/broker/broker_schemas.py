from datetime import datetime
from typing import Literal

from pydantic import field_validator, model_validator

from chattocodex.shared.contracts.orchestration_schemas import (
    CapabilityDigest,
    ObjectiveText,
    Revision,
    RoleText,
    RunId,
    RunView,
    TaskView,
    WorkerHandle,
    WorkerView,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel
from chattocodex.shared.contracts.types.coordination.broker_types import (
    BrokerConsumerId,
    BrokerTaskId,
    SlotLabel,
    WorkerSlotId,
)
from chattocodex.shared.element_collect.broker_enums import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)


class BrokerBoundaryModel(StrictSchemaModel):
    pass


class BrokerTimestampedRecord(BrokerBoundaryModel):
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
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
    external_published: bool = False

    @model_validator(mode="after")
    def validate_assignment_shape(self) -> BrokerTaskRecord:
        """Validate assignment shape. Args: None. Returns: BrokerTaskRecord."""
        status = BrokerTaskStatus(self.status)
        assignment = (
            self.assigned_slot_id,
            self.assigned_worker_id,
            self.assigned_task_id,
        )
        if status is BrokerTaskStatus.QUEUED and any(
            value is not None for value in assignment
        ):
            raise ValueError("queued broker tasks cannot retain an assignment")
        if status is BrokerTaskStatus.CLAIMED and any(
            value is None for value in assignment
        ):
            raise ValueError("claimed broker tasks require a complete assignment")
        return self


class WorkerSlotRecord(BrokerTimestampedRecord):
    schema_version: Literal[1] = 1
    slot_id: WorkerSlotId
    project_id: ProjectId
    broker_consumer_id: BrokerConsumerId | None = None
    slot_handle_digest: CapabilityDigest
    label: SlotLabel | None
    status: WorkerSlotStatus
    current_broker_task_id: BrokerTaskId | None
    current_run_id: RunId | None
    current_worker_id: str | None
    current_task_id: str | None
    revision: Revision
    broker_term: int | None = None
    broker_group_generation: int | None = None
    broker_lease_id: str | None = None
    broker_lease_epoch: int | None = None
    broker_lease_expires_at_ms: int | None = None

    @model_validator(mode="after")
    def validate_assignment_shape(self) -> WorkerSlotRecord:
        """Validate assignment shape. Args: None. Returns: WorkerSlotRecord."""
        status = WorkerSlotStatus(self.status)
        assignment = (
            self.current_broker_task_id,
            self.current_run_id,
            self.current_worker_id,
            self.current_task_id,
        )
        if status is WorkerSlotStatus.IDLE and any(
            value is not None for value in assignment
        ):
            raise ValueError("idle worker slots cannot retain an assignment")
        if status is WorkerSlotStatus.BUSY and any(
            value is None for value in assignment
        ):
            raise ValueError("busy worker slots require a complete assignment")
        broker_lease = (
            self.broker_term,
            self.broker_group_generation,
            self.broker_lease_id,
            self.broker_lease_epoch,
            self.broker_lease_expires_at_ms,
        )
        if status is WorkerSlotStatus.IDLE and any(
            value is not None for value in broker_lease
        ):
            raise ValueError("idle worker slots cannot retain an external Broker lease")
        if any(value is not None for value in broker_lease) and any(
            value is None for value in broker_lease
        ):
            raise ValueError(
                "external Broker lease metadata must be complete when present"
            )
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

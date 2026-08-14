from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from chattocodex.orchestration.orchestration_dto import RunState, TaskState, WorkerState


class BrokerTaskStatus(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in {BrokerTaskStatus.COMPLETED, BrokerTaskStatus.FAILED}


class WorkerSlotStatus(StrEnum):
    IDLE = "idle"
    BUSY = "busy"


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerTaskState:
    schema_version: int
    broker_task_id: str
    run_id: str
    objective: str
    role: str | None
    depends_on: tuple[str, ...]
    status: BrokerTaskStatus
    assigned_slot_id: str | None
    assigned_worker_id: str | None
    assigned_task_id: str | None
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerSlotState:
    schema_version: int
    slot_id: str
    project_id: str
    slot_handle_digest: str
    label: str | None
    status: WorkerSlotStatus
    current_broker_task_id: str | None
    current_run_id: str | None
    current_worker_id: str | None
    current_task_id: str | None
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class CreatedWorkerSlot:
    state: WorkerSlotState
    slot_handle: str


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerAssignment:
    broker_task: BrokerTaskState
    slot: WorkerSlotState
    run: RunState
    worker: WorkerState
    task: TaskState
    worker_handle: str


@dataclass(frozen=True, slots=True, kw_only=True)
class FinishedAssignment:
    broker_task: BrokerTaskState
    slot: WorkerSlotState
    task: TaskState


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerAcquireResult:
    assignment: BrokerAssignment | None
    slot: WorkerSlotState
    resumed: bool
    released_previous: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerRunSnapshot:
    run: RunState
    broker_tasks: tuple[BrokerTaskState, ...]
    slots: tuple[WorkerSlotState, ...]
    ready_count: int
    queued_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int
    is_complete: bool

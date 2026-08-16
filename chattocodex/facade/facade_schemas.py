from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from chattocodex.schemas.broker_schemas import (
    BrokerRunStatusSuccess,
    BrokerTaskView,
    SlotLabel,
    WaitSeconds,
    WorkerAssignmentFinishSuccess,
    WorkerAssignmentView,
    WorkerSlotHandle,
    WorkerSlotId,
    WorkerSlotView,
)
from chattocodex.schemas.orchestration_schemas import GoalText, RunHandle, RunView
from chattocodex.schemas.project_schemas import ProjectId
from chattocodex.schemas.wake_schemas import WakeTargetId
from chattocodex.wake.worker_wake_capability_dto import WorkerWakeTransport

TaskKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]
FacadeObjective = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4_000),
]
FacadeRole = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=120),
]


class FacadeBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class DelegationTaskInput(FacadeBoundaryModel):
    key: TaskKey
    objective: FacadeObjective
    role: FacadeRole | None = None
    depends_on: tuple[TaskKey, ...] = ()


class DelegateWorkRequest(FacadeBoundaryModel):
    goal: GoalText
    tasks: tuple[DelegationTaskInput, ...] = Field(min_length=1, max_length=32)
    requested_parallelism: int | None = Field(default=None, ge=1, le=32)
    project_id: ProjectId | None = None


class DelegatedTaskView(FacadeBoundaryModel):
    key: TaskKey
    broker_task: BrokerTaskView


class DelegateWorkSuccess(FacadeBoundaryModel):
    ok: Literal[True] = True
    run: RunView
    run_handle: RunHandle
    tasks: tuple[DelegatedTaskView, ...]


class ScheduledWakeCapabilityView(FacadeBoundaryModel):
    wake_target_id: WakeTargetId
    wake_revision: int = Field(ge=1)
    transport: WorkerWakeTransport
    expires_at: datetime
    scheduled_task_prompt: str


class PrepareScheduledWorkRequest(DelegateWorkRequest):
    pass


class PrepareScheduledWorkSuccess(FacadeBoundaryModel):
    ok: Literal[True] = True
    run: RunView
    run_handle: RunHandle
    tasks: tuple[DelegatedTaskView, ...]
    wake_capabilities: tuple[ScheduledWakeCapabilityView, ...]


class DelegationStatusRequest(FacadeBoundaryModel):
    run_id: str


class DelegationStatusSuccess(BrokerRunStatusSuccess):
    pass


class WorkRequest(FacadeBoundaryModel):
    project_id: ProjectId | None = None
    slot_id: WorkerSlotId | None = None
    slot_handle: WorkerSlotHandle | None = None
    label: SlotLabel | None = None
    wake_target_id: WakeTargetId | None = None
    wake_revision: int | None = Field(default=None, ge=1)
    wait_seconds: WaitSeconds = 0

    @model_validator(mode="after")
    def require_complete_slot_identity(self) -> WorkRequest:
        if (self.slot_id is None) != (self.slot_handle is None):
            raise ValueError("slot_id and slot_handle must be supplied together")
        if self.slot_id is None and (
            self.wake_target_id is None or self.wake_revision is None
        ):
            raise ValueError(
                "wake_target_id and wake_revision are required when creating a new Worker Slot"
            )
        if self.wake_revision is not None and self.wake_target_id is None:
            raise ValueError("wake_revision requires wake_target_id")
        return self


class WorkSuccess(FacadeBoundaryModel):
    ok: Literal[True] = True
    slot: WorkerSlotView
    slot_handle: WorkerSlotHandle
    assignment: WorkerAssignmentView | None
    registered: bool
    resumed: bool
    released_previous: bool
    timed_out: bool = False


class WorkDoneRequest(FacadeBoundaryModel):
    slot_id: WorkerSlotId
    slot_handle: WorkerSlotHandle


class WorkDoneSuccess(FacadeBoundaryModel):
    ok: Literal[True] = True
    slot: WorkerSlotView
    finished: WorkerAssignmentFinishSuccess | None
    already_idle: bool

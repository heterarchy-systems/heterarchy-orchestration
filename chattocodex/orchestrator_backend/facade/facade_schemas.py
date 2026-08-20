from datetime import datetime
from typing import Annotated, Literal

from pydantic import model_validator

from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_dto import (
    WorkerWakeTransport,
)
from chattocodex.shared.contracts.broker.broker_schemas import (
    BrokerRunStatusSuccess,
    BrokerTaskView,
    SlotLabel,
    WorkerAssignmentFinishSuccess,
    WorkerAssignmentView,
    WorkerSlotId,
    WorkerSlotView,
)
from chattocodex.shared.contracts.orchestration_schemas import (
    GoalText,
    RunHandle,
    RunView,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.coordination.broker_types import (
    WaitSeconds,
    WorkerSlotHandle,
)
from chattocodex.shared.contracts.types.coordination.facade_types import (
    FacadeObjective,
    FacadeRole,
    TaskKey,
)
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetId


class FacadeBoundaryModel(StrictSchemaModel):
    pass


class DelegationTaskInput(FacadeBoundaryModel):
    key: TaskKey
    objective: FacadeObjective
    role: FacadeRole | None = None
    depends_on: tuple[TaskKey, ...] = ()


class DelegateWorkRequest(FacadeBoundaryModel):
    goal: GoalText
    tasks: Annotated[
        tuple[DelegationTaskInput, ...],
        described_field(
            description="Tasks for this delegate work request.",
            min_length=1,
            max_length=32,
        ),
    ]
    requested_parallelism: Annotated[
        int | None,
        described_field(
            description="Requested parallelism for this delegate work request.",
            ge=1,
            le=32,
        ),
    ] = None
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
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this scheduled wake capability view.", ge=1
        ),
    ]
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
    wake_revision: Annotated[
        int | None,
        described_field(description="Wake revision for this work request.", ge=1),
    ] = None
    wait_seconds: WaitSeconds = 0

    @model_validator(mode="after")
    def require_complete_slot_identity(self) -> WorkRequest:
        """Require complete slot identity. Args: None. Returns: WorkRequest."""
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

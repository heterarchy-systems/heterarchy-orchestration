from dataclasses import dataclass

from chattocodex.broker.broker_dto import (
    BrokerAssignment,
    BrokerRunSnapshot,
    BrokerTaskState,
    FinishedAssignment,
    WorkerSlotState,
)
from chattocodex.orchestration.orchestration_dto import CreatedRun
from chattocodex.wake.worker_wake_capability_dto import WorkerWakeCapabilityState


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationTaskSpec:
    key: str
    objective: str
    role: str | None
    depends_on: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegatedTask:
    key: str
    broker_task: BrokerTaskState


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationStarted:
    run: CreatedRun
    tasks: tuple[DelegatedTask, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduledDelegationStarted:
    run: CreatedRun
    tasks: tuple[DelegatedTask, ...]
    wake_capabilities: tuple[WorkerWakeCapabilityState, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerWork:
    slot: WorkerSlotState
    slot_handle: str
    assignment: BrokerAssignment | None
    registered: bool
    resumed: bool
    released_previous: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerWorkDone:
    slot: WorkerSlotState
    finished: FinishedAssignment | None
    already_idle: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class DelegationStatus:
    snapshot: BrokerRunSnapshot

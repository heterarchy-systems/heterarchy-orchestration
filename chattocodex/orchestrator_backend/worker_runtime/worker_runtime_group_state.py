from dataclasses import dataclass
from datetime import datetime

from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus
from chattocodex.orchestrator_backend.worker_runtime.capacity.capacity_decision import (
    CapacityDecision,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_enums import (
    WorkerRuntimeAvailability,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerRuntimeGroupState:
    """Runtime-topology projection for recoverable Worker wake targets."""

    schema_version: int
    project_id: str
    topology_generation: int
    wake_target_ids: tuple[str, ...]
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerRuntimeMember:
    wake_target_id: str
    availability: WorkerRuntimeAvailability
    wake_status: WakeTargetStatus
    bound_slot_id: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerRuntimeSnapshot:
    group: WorkerRuntimeGroupState
    members: tuple[WorkerRuntimeMember, ...]
    ready_count: int
    recovered_slot_ids: tuple[str, ...]
    wake_transition_count: int
    topology_changed: bool
    capacity_decision: CapacityDecision | None = None

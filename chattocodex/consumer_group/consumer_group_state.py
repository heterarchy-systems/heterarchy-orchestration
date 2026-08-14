from dataclasses import dataclass
from datetime import datetime

from chattocodex.consumer_group.capacity_decision import CapacityDecision
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.worker_runtime.worker_runtime_presence_state import (
    WorkerRuntimeAvailability,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumerGroupState:
    schema_version: int
    project_id: str
    generation: int
    member_ids: tuple[str, ...]
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumerGroupMember:
    wake_target_id: str
    availability: WorkerRuntimeAvailability
    wake_status: WakeTargetStatus
    bound_slot_id: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumerGroupSnapshot:
    group: ConsumerGroupState
    members: tuple[ConsumerGroupMember, ...]
    ready_count: int
    recovered_slot_ids: tuple[str, ...]
    wake_transition_count: int
    membership_changed: bool
    capacity_decision: CapacityDecision | None = None

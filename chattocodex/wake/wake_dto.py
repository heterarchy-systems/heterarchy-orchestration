from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class WakeTargetStatus(StrEnum):
    IDLE = "idle"
    WAKING = "waking"
    CLAIMING = "claiming"
    BUSY = "busy"


@dataclass(frozen=True, slots=True, kw_only=True)
class WakeTargetState:
    schema_version: int
    wake_target_id: str
    project_id: str
    registered_connector_epoch: int
    status: WakeTargetStatus
    bound_slot_id: str | None
    wake_revision: int
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WakeTargetRetirement:
    """Compare-and-swap input for one project-scoped target retirement."""

    wake_target_id: str
    expected_revision: int


@dataclass(frozen=True, slots=True, kw_only=True)
class WakePresenceState:
    schema_version: int
    wake_target_id: str
    last_seen_at: datetime

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class WorkerRuntimeAvailability(StrEnum):
    ACTIVE = "active"
    DORMANT = "dormant"
    OFFLINE = "offline"


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerRuntimePresenceState:
    schema_version: int
    wake_target_id: str
    last_seen_at: datetime

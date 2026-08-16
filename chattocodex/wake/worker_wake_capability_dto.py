from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class WorkerWakeCapabilityStatus(StrEnum):
    ISSUED = "issued"
    CLAIMING = "claiming"
    CONSUMED = "consumed"


class WorkerWakeTransport(StrEnum):
    SCHEDULER = "scheduler"


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerWakeCapabilityState:
    schema_version: int
    wake_target_id: str
    project_id: str
    run_id: str
    registered_connector_epoch: int
    wake_revision: int
    transport: WorkerWakeTransport
    status: WorkerWakeCapabilityStatus
    bound_slot_id: str | None
    revision: int
    issued_at: datetime
    expires_at: datetime
    updated_at: datetime

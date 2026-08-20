from dataclasses import dataclass
from datetime import datetime

from chattocodex.shared.element_collect.wake_enums import (
    WorkerWakeCapabilityStatus,
    WorkerWakeTransport,
)


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

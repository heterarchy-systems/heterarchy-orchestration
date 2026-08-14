from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WakeDeliveryLeaseState:
    schema_version: int
    wake_target_id: str
    wake_revision: int
    lease_started_at: datetime | None
    cooldown_until: datetime | None
    failure_streak: int
    updated_at: datetime

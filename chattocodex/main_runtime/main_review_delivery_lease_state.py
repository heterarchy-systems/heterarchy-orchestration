from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewDeliveryLeaseState:
    schema_version: int
    main_target_id: str
    wake_revision: int
    lease_started_at: datetime | None
    cooldown_until: datetime | None
    failure_streak: int
    updated_at: datetime

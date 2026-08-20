from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator

from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetId


class WakeDeliveryBoundaryModel(StrictSchemaModel):
    pass


class WakeDeliveryLeaseRecord(WakeDeliveryBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this wake delivery lease record.", ge=0
        ),
    ]
    lease_started_at: datetime | None = None
    cooldown_until: datetime | None = None
    failure_streak: Annotated[
        int,
        described_field(
            description="Failure streak for this wake delivery lease record.", ge=0
        ),
    ]
    updated_at: datetime

    @field_validator("lease_started_at", "cooldown_until", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Require timezone. Args: value. Returns: datetime | None."""
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

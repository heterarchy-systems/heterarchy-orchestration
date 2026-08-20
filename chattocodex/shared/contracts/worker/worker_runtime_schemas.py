from datetime import datetime
from typing import Literal

from pydantic import field_validator

from chattocodex.shared.contracts.schema_base import StrictSchemaModel
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetId


class WorkerRuntimeBoundaryModel(StrictSchemaModel):
    pass


class WorkerRuntimePresenceRecord(WorkerRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    last_seen_at: datetime

    @field_validator("last_seen_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

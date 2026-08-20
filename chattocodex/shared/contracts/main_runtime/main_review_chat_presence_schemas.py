from datetime import datetime
from typing import Literal

from pydantic import field_validator

from chattocodex.shared.contracts.main_runtime.main_runtime_schemas import (
    MainRuntimeBoundaryModel,
    MainTargetId,
)


class MainReviewChatPresenceRecord(MainRuntimeBoundaryModel):
    schema_version: Literal[1] = 1
    main_target_id: MainTargetId
    last_seen_at: datetime

    @field_validator("last_seen_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

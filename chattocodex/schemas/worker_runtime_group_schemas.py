from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from chattocodex.schemas.project_schemas import ProjectId

WakeTargetId = Annotated[
    str,
    StringConstraints(pattern=r"^wtgt_[0-9a-f]{32}$"),
]


class WorkerRuntimeGroupRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    schema_version: Literal[1] = 1
    project_id: ProjectId
    topology_generation: int = Field(ge=0)
    wake_target_ids: tuple[WakeTargetId, ...]
    revision: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

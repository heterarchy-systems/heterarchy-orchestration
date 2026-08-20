from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator

from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.control_plane.wake_types import WakeTargetId


class WorkerRuntimeGroupRecord(StrictSchemaModel):
    schema_version: Annotated[
        Literal[1],
        described_field(
            description="Storage schema version for this runtime group record."
        ),
    ] = 1
    project_id: Annotated[
        ProjectId,
        described_field(description="Canonical project owning this runtime group."),
    ]
    topology_generation: Annotated[
        int,
        described_field(
            description="Topology generation for this worker runtime group record.",
            ge=0,
        ),
    ]
    wake_target_ids: Annotated[
        tuple[WakeTargetId, ...],
        described_field(
            description="Ordered wake targets currently represented by this runtime group."
        ),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this worker runtime group record.", ge=0
        ),
    ]
    created_at: Annotated[
        datetime,
        described_field(description="Timezone-aware record creation timestamp."),
    ]
    updated_at: Annotated[
        datetime,
        described_field(description="Timezone-aware timestamp of the latest mutation."),
    ]

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator

from chattocodex.shared.contracts.broker.broker_schemas import WorkerSlotId
from chattocodex.shared.contracts.orchestration_schemas import RunId
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetId
from chattocodex.shared.element_collect.wake_enums import (
    WorkerWakeCapabilityStatus,
    WorkerWakeTransport,
)


class WorkerWakeCapabilityRecord(StrictSchemaModel):
    schema_version: Annotated[
        Literal[1],
        described_field(
            description="Storage schema version for this wake capability record."
        ),
    ] = 1
    wake_target_id: Annotated[
        WakeTargetId,
        described_field(description="Opaque wake target receiving this capability."),
    ]
    project_id: Annotated[
        ProjectId,
        described_field(
            description="Canonical project associated with the wake target."
        ),
    ]
    run_id: Annotated[
        RunId,
        described_field(description="Run authorized to consume this wake capability."),
    ]
    registered_connector_epoch: Annotated[
        int,
        described_field(
            description="Registered connector epoch for this worker wake capability record.",
            ge=0,
        ),
    ]
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this worker wake capability record.", ge=1
        ),
    ]
    transport: Annotated[
        WorkerWakeTransport,
        described_field(
            description="Delivery transport authorized for this capability."
        ),
    ]
    status: Annotated[
        WorkerWakeCapabilityStatus,
        described_field(
            description="Current lifecycle status of this wake capability."
        ),
    ]
    bound_slot_id: Annotated[
        WorkerSlotId | None,
        described_field(
            description="Worker slot bound by a successful claim, or null before binding."
        ),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this worker wake capability record.",
            ge=0,
        ),
    ]
    issued_at: Annotated[
        datetime,
        described_field(description="Timezone-aware capability issuance timestamp."),
    ]
    expires_at: Annotated[
        datetime,
        described_field(description="Timezone-aware capability expiration timestamp."),
    ]
    updated_at: Annotated[
        datetime,
        described_field(description="Timezone-aware timestamp of the latest mutation."),
    ]

    @field_validator("issued_at", "expires_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

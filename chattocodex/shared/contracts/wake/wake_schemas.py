from datetime import datetime
from typing import Annotated, Literal

from pydantic import field_validator, model_validator

from chattocodex.shared.contracts.broker.broker_schemas import WorkerSlotId
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.control_plane.wake_types import WakeTargetId
from chattocodex.shared.element_collect.wake_enums import WakeTargetStatus


class WakeBoundaryModel(StrictSchemaModel):
    pass


class WakeTargetRecord(WakeBoundaryModel):
    schema_version: Literal[1] = 1
    wake_target_id: WakeTargetId
    project_id: ProjectId
    registered_connector_epoch: Annotated[
        int,
        described_field(
            description="Registered connector epoch for this wake target record.", ge=0
        ),
    ] = 0
    status: WakeTargetStatus
    bound_slot_id: WorkerSlotId | None
    wake_revision: Annotated[
        int,
        described_field(description="Wake revision for this wake target record.", ge=0),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this wake target record.", ge=0
        ),
    ]
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone. Args: value. Returns: datetime."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_busy_binding(self) -> WakeTargetRecord:
        """Validate busy binding. Args: None. Returns: WakeTargetRecord."""
        status = WakeTargetStatus(self.status)
        if status is WakeTargetStatus.BUSY and self.bound_slot_id is None:
            raise ValueError("busy wake targets require a bound worker slot")
        return self


class WakePresenceRecord(WakeBoundaryModel):
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


class WakeProjectView(WakeBoundaryModel):
    project_id: ProjectId
    name: str


class WakeProjectsResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    projects: tuple[WakeProjectView, ...]


class WakeTargetRegisterRequest(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    project_id: ProjectId


class WakeTargetPollRequest(WakeBoundaryModel):
    wake_target_id: WakeTargetId


class WakeTargetRetireRequest(WakeBoundaryModel):
    expected_revision: Annotated[
        int,
        described_field(
            description="Expected revision used to guard updates to this wake target retire request.",
            ge=0,
        ),
    ]


class WakeTargetRetirementItem(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    expected_revision: Annotated[
        int,
        described_field(
            description="Expected revision used to guard updates to this wake target retirement item.",
            ge=0,
        ),
    ]


class WakeTargetRetireBatchRequest(WakeBoundaryModel):
    project_id: ProjectId
    targets: Annotated[
        tuple[WakeTargetRetirementItem, ...],
        described_field(
            description="Targets for this wake target retire batch request.",
            min_length=1,
            max_length=32,
        ),
    ]

    @field_validator("targets")
    @classmethod
    def require_unique_target_ids(
        cls, value: tuple[WakeTargetRetirementItem, ...]
    ) -> tuple[WakeTargetRetirementItem, ...]:
        """Require unique target ids. Args: value. Returns: tuple[WakeTargetRetirementItem, ...]."""
        target_ids = tuple(item.wake_target_id for item in value)
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("wake_target_id values must be unique")
        return value


class WakeTargetView(WakeBoundaryModel):
    wake_target_id: WakeTargetId
    project_id: ProjectId
    registered_connector_epoch: Annotated[
        int,
        described_field(
            description="Registered connector epoch for this wake target view.", ge=0
        ),
    ]
    status: WakeTargetStatus
    wake_revision: Annotated[
        int,
        described_field(description="Wake revision for this wake target view.", ge=0),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this wake target view.", ge=0
        ),
    ]
    updated_at: datetime


class WakeTargetResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    target: WakeTargetView


class WakeTargetRetireResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    wake_target_id: WakeTargetId
    retired_revision: Annotated[
        int,
        described_field(
            description="Retired revision for this wake target retire response.", ge=0
        ),
    ]


class WakeTargetRetireBatchResponse(WakeBoundaryModel):
    ok: Literal[True] = True
    project_id: ProjectId
    retired_wake_target_ids: tuple[WakeTargetId, ...]

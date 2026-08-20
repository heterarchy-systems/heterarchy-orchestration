"""Pydantic contracts for the opaque local Worker provisioning boundary."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import StringConstraints, field_validator

from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.workspace.provisioning_types import (
    ProvisioningRequestId,
)
from chattocodex.shared.element_collect.provisioning_enums import (
    ProvisioningProgressStatusValue,
    ProvisioningRequestStatusValue,
)

ProvisioningConversationId = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=512),
]
ProvisioningFailureReason = Annotated[
    str,
    StringConstraints(
        strict=True, strip_whitespace=True, min_length=1, max_length=1024
    ),
]


class WorkerProvisioningBoundaryModel(StrictSchemaModel):
    """Strict immutable contracts for the local provisioning adapter."""


class WorkerProvisioningStatusView(WorkerProvisioningBoundaryModel):
    """Opaque lifecycle view returned to the local browser adapter."""

    status: ProvisioningRequestStatusValue


class WorkerProvisioningDemandRequest(WorkerProvisioningBoundaryModel):
    """Backend capacity deficit submitted by the capacity integration seam."""

    project_id: ProjectId
    desired_capacity: Annotated[
        int,
        described_field(
            description="Desired capacity for this worker provisioning demand request.",
            ge=0,
        ),
    ]
    spawn_needed: Annotated[
        int,
        described_field(
            description="Spawn needed for this worker provisioning demand request.",
            ge=0,
        ),
    ]
    binding_revision: Annotated[
        int | None,
        described_field(
            description="Binding revision for this worker provisioning demand request.",
            ge=0,
        ),
    ] = None


class WorkerProvisioningDemandView(WorkerProvisioningBoundaryModel):
    """Opaque view of durable provisioning demand."""

    project_id: ProjectId
    desired_capacity: Annotated[
        int,
        described_field(
            description="Desired capacity for this worker provisioning demand view.",
            ge=0,
        ),
    ]
    spawn_needed: Annotated[
        int,
        described_field(
            description="Spawn needed for this worker provisioning demand view.", ge=0
        ),
    ]
    binding_revision: Annotated[
        int | None,
        described_field(
            description="Binding revision for this worker provisioning demand view.",
            ge=0,
        ),
    ] = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this worker provisioning demand view.",
            ge=0,
        ),
    ]
    updated_at: datetime

    @field_validator("updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require a timezone-aware demand timestamp.

        Args:
            value: Timestamp emitted by durable provisioning state.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("provisioning demand timestamp must be timezone-aware")
        return value


class WorkerProvisioningRequestView(WorkerProvisioningBoundaryModel):
    """Opaque request view containing no Task, Role, Run, or DAG semantics."""

    request_id: ProvisioningRequestId
    project_id: ProjectId
    status: ProvisioningRequestStatusValue
    admission_revision: Annotated[
        int | None,
        described_field(
            description="Admission revision for this worker provisioning request view.",
            ge=0,
        ),
    ] = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this worker provisioning request view.",
            ge=0,
        ),
    ]
    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    conversation_id: ProvisioningConversationId | None = None
    wake_target_id: (
        Annotated[
            str,
            StringConstraints(strict=True, pattern=r"^wtgt_[0-9a-f]{32}$"),
        ]
        | None
    ) = None
    failure_reason: ProvisioningFailureReason | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone-aware request timestamps.

        Args:
            value: Timestamp emitted by durable provisioning state.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("provisioning request timestamps must be timezone-aware")
        return value


class WorkerProvisioningNextResponse(WorkerProvisioningBoundaryModel):
    """Response for one explicitly admitted browser provisioning request."""

    ok: Literal[True] = True
    request: WorkerProvisioningRequestView | None


class WorkerProvisioningDemandResponse(WorkerProvisioningBoundaryModel):
    """Response containing the durable capacity deficit snapshot."""

    ok: Literal[True] = True
    demand: WorkerProvisioningDemandView


class WorkerProvisioningRequestResponse(WorkerProvisioningBoundaryModel):
    """Response containing one durable provisioning request."""

    ok: Literal[True] = True
    request: WorkerProvisioningRequestView


class WorkerProvisioningRegistrationRequest(WorkerProvisioningBoundaryModel):
    """Opaque callback proving a persistent conversation and wake target exist."""

    conversation_id: ProvisioningConversationId
    wake_target_id: Annotated[
        str,
        StringConstraints(strict=True, pattern=r"^wtgt_[0-9a-f]{32}$"),
    ]
    admission_revision: Annotated[
        int,
        described_field(
            description="Admission revision for this worker provisioning registration request.",
            ge=0,
        ),
    ]


class WorkerProvisioningProgressRequest(WorkerProvisioningBoundaryModel):
    """Opaque callback advancing a request through browser provisioning phases."""

    status: ProvisioningProgressStatusValue
    admission_revision: Annotated[
        int,
        described_field(
            description="Admission revision for this worker provisioning progress request.",
            ge=0,
        ),
    ]


class WorkerProvisioningFailureRequest(WorkerProvisioningBoundaryModel):
    """Opaque callback reporting a failed or rate-limited browser attempt."""

    admission_revision: Annotated[
        int,
        described_field(
            description="Admission revision for this worker provisioning failure request.",
            ge=0,
        ),
    ]
    reason: ProvisioningFailureReason
    rate_limited: bool = False


class WorkerProvisioningCancelRequest(WorkerProvisioningBoundaryModel):
    """Opaque request cancellation contract."""

    admission_revision: Annotated[
        int | None,
        described_field(
            description="Admission revision for this worker provisioning cancel request.",
            ge=0,
        ),
    ] = None


class WorkerProvisioningRetryRequest(WorkerProvisioningBoundaryModel):
    """Explicit operator retry guarded by the latest request revision."""

    expected_revision: Annotated[
        int,
        described_field(
            description="Expected revision used to guard updates to this worker provisioning retry request.",
            ge=0,
        ),
    ]


class WorkerProvisioningStatusResponse(WorkerProvisioningBoundaryModel):
    """Response for local Control Panel provisioning status inspection."""

    ok: Literal[True] = True
    demands: tuple[WorkerProvisioningDemandView, ...]
    requests: tuple[WorkerProvisioningRequestView, ...]


class WorkerProvisioningFailureResponse(WorkerProvisioningBoundaryModel):
    """Response carrying a bounded provider retry delay."""

    ok: Literal[True] = True
    retry_after_ms: Annotated[
        int | None,
        described_field(
            description="Retry after duration in milliseconds for this worker provisioning failure response.",
            ge=0,
        ),
    ] = None


__all__ = [
    "ProjectBindingAdapterMetadata",
    "ProvisioningConversationId",
    "ProvisioningRequestId",
    "ProvisioningRequestStatusValue",
    "WorkerProvisioningBoundaryModel",
    "WorkerProvisioningCancelRequest",
    "WorkerProvisioningDemandRequest",
    "WorkerProvisioningDemandResponse",
    "WorkerProvisioningDemandView",
    "WorkerProvisioningFailureRequest",
    "WorkerProvisioningFailureResponse",
    "WorkerProvisioningNextResponse",
    "WorkerProvisioningProgressRequest",
    "WorkerProvisioningRegistrationRequest",
    "WorkerProvisioningRequestResponse",
    "WorkerProvisioningRequestView",
    "WorkerProvisioningRetryRequest",
    "WorkerProvisioningStatusResponse",
    "WorkerProvisioningStatusView",
]

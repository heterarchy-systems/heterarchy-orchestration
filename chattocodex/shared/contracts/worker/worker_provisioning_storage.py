"""Strict durable JSON records for Worker provisioning state."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import StringConstraints, field_validator

from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.control_plane.wake_types import WakeTargetId
from chattocodex.shared.contracts.types.workspace.provisioning_types import (
    ProvisioningRequestId,
)
from chattocodex.shared.element_collect.provisioning_enums import (
    ProvisioningRequestStatus,
)

ConversationId = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=512),
]
FailureReason = Annotated[
    str,
    StringConstraints(
        strict=True, strip_whitespace=True, min_length=1, max_length=1024
    ),
]


class ProvisioningStorageModel(StrictSchemaModel):
    """Strict immutable base for filesystem provisioning records."""


class ProvisioningDemandRecord(ProvisioningStorageModel):
    """Durable capacity deficit record."""

    schema_version: Literal[1] = 1
    project_id: ProjectId
    desired_capacity: Annotated[
        int,
        described_field(
            description="Desired capacity for this provisioning demand record.", ge=0
        ),
    ]
    spawn_needed: Annotated[
        int,
        described_field(
            description="Spawn needed for this provisioning demand record.", ge=0
        ),
    ]
    binding_revision: Annotated[
        int | None,
        described_field(
            description="Binding revision for this provisioning demand record.", ge=0
        ),
    ] = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this provisioning demand record.", ge=0
        ),
    ]
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone-aware demand timestamps.

        Args:
            value: Timestamp read from durable state.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("provisioning demand timestamp must be timezone-aware")
        return value


class ProvisioningRequestRecord(ProvisioningStorageModel):
    """Durable provisioning lifecycle record."""

    schema_version: Literal[1] = 1
    request_id: ProvisioningRequestId
    project_id: ProjectId
    binding_revision: Annotated[
        int | None,
        described_field(
            description="Binding revision for this provisioning request record.", ge=0
        ),
    ] = None
    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    status: ProvisioningRequestStatus
    admission_revision: Annotated[
        int | None,
        described_field(
            description="Admission revision for this provisioning request record.", ge=0
        ),
    ] = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this provisioning request record.", ge=0
        ),
    ]
    conversation_id: ConversationId | None = None
    wake_target_id: WakeTargetId | None = None
    failure_reason: FailureReason | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require timezone-aware request timestamps.

        Args:
            value: Timestamp read from durable state.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("provisioning request timestamp must be timezone-aware")
        return value


class ProvisioningAdmissionRecord(ProvisioningStorageModel):
    """Durable global open/provision admission record."""

    schema_version: Literal[1] = 1
    in_flight_request_id: ProvisioningRequestId | None = None
    last_admitted_at: datetime | None = None
    next_admission_at: datetime | None = None
    backoff_attempt: Annotated[
        int,
        described_field(
            description="Backoff attempt for this provisioning admission record.",
            ge=0,
            le=3,
        ),
    ]
    backoff_until: datetime | None = None
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this provisioning admission record.",
            ge=0,
        ),
    ]

    @field_validator("last_admitted_at", "next_admission_at", "backoff_until")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        """Require timezone-aware admission timestamps.

        Args:
            value: Optional timestamp read from durable state.

        Returns:
            The validated timestamp or ``None``.

        Raises:
            ValueError: If a supplied timestamp is timezone-naive.
        """
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("provisioning admission timestamp must be timezone-aware")
        return value


class ProvisioningStoreRecord(ProvisioningStorageModel):
    """Atomic JSON envelope for all provisioning state."""

    schema_version: Literal[1] = 1
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this provisioning store record.", ge=0
        ),
    ]
    demands: tuple[ProvisioningDemandRecord, ...]
    requests: tuple[ProvisioningRequestRecord, ...]
    admission: ProvisioningAdmissionRecord
    updated_at: datetime

    @field_validator("updated_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require a timezone-aware store timestamp.

        Args:
            value: Timestamp read from durable state.

        Returns:
            The validated timestamp.

        Raises:
            ValueError: If ``value`` is timezone-naive.
        """
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("provisioning store timestamp must be timezone-aware")
        return value

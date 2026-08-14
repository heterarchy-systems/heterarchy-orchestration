"""Strict durable JSON records for Worker provisioning state."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from chattocodex.provisioning.provisioning_dto import ProvisioningRequestStatus
from chattocodex.schemas.project_binding_schemas import ProjectBindingAdapterMetadata
from chattocodex.schemas.project_schemas import ProjectId

ProvisioningRequestId = Annotated[
    str,
    StringConstraints(pattern=r"^prq_[0-9a-f]{32}$"),
]
WakeTargetId = Annotated[
    str,
    StringConstraints(pattern=r"^wtgt_[0-9a-f]{32}$"),
]
ConversationId = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=512),
]
FailureReason = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=1024),
]


class ProvisioningStorageModel(BaseModel):
    """Strict immutable base for filesystem provisioning records."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class ProvisioningDemandRecord(ProvisioningStorageModel):
    """Durable capacity deficit record."""

    schema_version: Literal[1] = 1
    project_id: ProjectId
    desired_capacity: int = Field(ge=0)
    spawn_needed: int = Field(ge=0)
    binding_revision: int | None = Field(default=None, ge=0)
    revision: int = Field(ge=0)
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
    binding_revision: int | None = Field(default=None, ge=0)
    adapter_metadata: ProjectBindingAdapterMetadata | None = None
    status: ProvisioningRequestStatus
    admission_revision: int | None = Field(default=None, ge=0)
    revision: int = Field(ge=0)
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
    backoff_attempt: int = Field(ge=0, le=3)
    backoff_until: datetime | None = None
    revision: int = Field(ge=0)

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
    revision: int = Field(ge=0)
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

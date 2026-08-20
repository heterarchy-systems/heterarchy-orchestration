"""Internal values for project-bound Worker provisioning."""

from dataclasses import dataclass
from datetime import datetime

from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingAdapterMetadataState,
)
from chattocodex.shared.element_collect.provisioning_enums import (
    ProvisioningRequestStatus,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisioningDemandState:
    """Durable capacity deficit for one canonical local project."""

    schema_version: int
    project_id: str
    desired_capacity: int
    spawn_needed: int
    binding_revision: int | None
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisioningRequestState:
    """Durable state for one project-bound Worker creation attempt."""

    schema_version: int
    request_id: str
    project_id: str
    binding_revision: int | None
    adapter_metadata: ProjectBindingAdapterMetadataState | None
    status: ProvisioningRequestStatus
    admission_revision: int | None
    revision: int
    conversation_id: str | None
    wake_target_id: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisioningAdmissionState:
    """Global durable admission gate for browser project/chat creation."""

    schema_version: int
    in_flight_request_id: str | None
    last_admitted_at: datetime | None
    next_admission_at: datetime | None
    backoff_attempt: int
    backoff_until: datetime | None
    revision: int


@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisioningStoreState:
    """Atomic snapshot containing all provisioning records and the admission gate."""

    schema_version: int
    revision: int
    demands: tuple[ProvisioningDemandState, ...]
    requests: tuple[ProvisioningRequestState, ...]
    admission: ProvisioningAdmissionState
    updated_at: datetime

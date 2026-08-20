from enum import StrEnum


class ProvisioningRequestStatus(StrEnum):
    """Internal provisioning request lifecycle states."""

    WAITING_ADMISSION = "waiting_admission"
    OPENING_PROJECT = "opening_project"
    CREATING_CHAT = "creating_chat"
    BOOTSTRAPPING = "bootstrapping"
    WAITING_REGISTRATION = "waiting_registration"
    ACTIVE = "active"
    RATE_LIMITED = "rate_limited"
    DEFERRED = "deferred"
    OPERATOR_BLOCKED = "operator_blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProvisioningRequestStatusValue(StrEnum):
    """Provisioning lifecycle values exposed by the browser-facing contract."""

    WAITING_ADMISSION = "waiting_admission"
    OPENING_PROJECT = "opening_project"
    CREATING_CHAT = "creating_chat"
    BOOTSTRAPPING = "bootstrapping"
    WAITING_REGISTRATION = "waiting_registration"
    ACTIVE = "active"
    RATE_LIMITED = "rate_limited"
    DEFERRED = "deferred"
    OPERATOR_BLOCKED = "operator_blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProvisioningProgressStatusValue(StrEnum):
    """Browser provisioning phases accepted by progress callbacks."""

    OPENING_PROJECT = "opening_project"
    CREATING_CHAT = "creating_chat"
    BOOTSTRAPPING = "bootstrapping"
    WAITING_REGISTRATION = "waiting_registration"

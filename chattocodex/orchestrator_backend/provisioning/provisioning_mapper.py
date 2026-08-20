"""Mappings between provisioning durable records, internal values, and API views."""

from chattocodex.orchestrator_backend.project_runtime.project_binding_mapper import (
    metadata_schema_from_state,
    metadata_state_from_schema,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningAdmissionState,
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.shared.contracts.worker.worker_provisioning_schemas import (
    ProvisioningRequestStatusValue,
    WorkerProvisioningDemandView,
    WorkerProvisioningRequestView,
)
from chattocodex.shared.contracts.worker.worker_provisioning_storage import (
    ProvisioningAdmissionRecord,
    ProvisioningDemandRecord,
    ProvisioningRequestRecord,
    ProvisioningStoreRecord,
)

_REQUEST_STATUS_VALUES: dict[
    ProvisioningRequestStatus, ProvisioningRequestStatusValue
] = {
    ProvisioningRequestStatus.WAITING_ADMISSION: ProvisioningRequestStatusValue.WAITING_ADMISSION,
    ProvisioningRequestStatus.OPENING_PROJECT: ProvisioningRequestStatusValue.OPENING_PROJECT,
    ProvisioningRequestStatus.CREATING_CHAT: ProvisioningRequestStatusValue.CREATING_CHAT,
    ProvisioningRequestStatus.BOOTSTRAPPING: ProvisioningRequestStatusValue.BOOTSTRAPPING,
    ProvisioningRequestStatus.WAITING_REGISTRATION: ProvisioningRequestStatusValue.WAITING_REGISTRATION,
    ProvisioningRequestStatus.ACTIVE: ProvisioningRequestStatusValue.ACTIVE,
    ProvisioningRequestStatus.RATE_LIMITED: ProvisioningRequestStatusValue.RATE_LIMITED,
    ProvisioningRequestStatus.DEFERRED: ProvisioningRequestStatusValue.DEFERRED,
    ProvisioningRequestStatus.OPERATOR_BLOCKED: ProvisioningRequestStatusValue.OPERATOR_BLOCKED,
    ProvisioningRequestStatus.FAILED: ProvisioningRequestStatusValue.FAILED,
    ProvisioningRequestStatus.CANCELLED: ProvisioningRequestStatusValue.CANCELLED,
}


def demand_from_record(record: ProvisioningDemandRecord) -> ProvisioningDemandState:
    """Convert a validated durable demand record to an internal value.

    Args:
        record: Validated provisioning demand record.

    Returns:
        Immutable internal demand state.
    """
    return ProvisioningDemandState(
        schema_version=record.schema_version,
        project_id=record.project_id,
        desired_capacity=record.desired_capacity,
        spawn_needed=record.spawn_needed,
        binding_revision=record.binding_revision,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def demand_to_record(state: ProvisioningDemandState) -> ProvisioningDemandRecord:
    """Convert an internal demand value to a durable record.

    Args:
        state: Immutable internal demand state.

    Returns:
        Validated durable demand record.
    """
    return ProvisioningDemandRecord(
        schema_version=1,
        project_id=state.project_id,
        desired_capacity=state.desired_capacity,
        spawn_needed=state.spawn_needed,
        binding_revision=state.binding_revision,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def request_from_record(record: ProvisioningRequestRecord) -> ProvisioningRequestState:
    """Convert a validated durable request record to an internal value.

    Args:
        record: Validated provisioning request record.

    Returns:
        Immutable internal request state.
    """
    return ProvisioningRequestState(
        schema_version=record.schema_version,
        request_id=record.request_id,
        project_id=record.project_id,
        binding_revision=record.binding_revision,
        adapter_metadata=metadata_state_from_schema(record.adapter_metadata),
        status=ProvisioningRequestStatus(record.status),
        admission_revision=record.admission_revision,
        revision=record.revision,
        conversation_id=record.conversation_id,
        wake_target_id=record.wake_target_id,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def request_to_record(state: ProvisioningRequestState) -> ProvisioningRequestRecord:
    """Convert an internal request value to a durable record.

    Args:
        state: Immutable internal request state.

    Returns:
        Validated durable request record.
    """
    return ProvisioningRequestRecord(
        schema_version=1,
        request_id=state.request_id,
        project_id=state.project_id,
        binding_revision=state.binding_revision,
        adapter_metadata=metadata_schema_from_state(state.adapter_metadata),
        status=state.status,
        admission_revision=state.admission_revision,
        revision=state.revision,
        conversation_id=state.conversation_id,
        wake_target_id=state.wake_target_id,
        failure_reason=state.failure_reason,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def admission_from_record(
    record: ProvisioningAdmissionRecord,
) -> ProvisioningAdmissionState:
    """Convert a validated admission record to an internal value.

    Args:
        record: Validated global admission record.

    Returns:
        Immutable internal admission state.
    """
    return ProvisioningAdmissionState(
        schema_version=record.schema_version,
        in_flight_request_id=record.in_flight_request_id,
        last_admitted_at=record.last_admitted_at,
        next_admission_at=record.next_admission_at,
        backoff_attempt=record.backoff_attempt,
        backoff_until=record.backoff_until,
        revision=record.revision,
    )


def admission_to_record(
    state: ProvisioningAdmissionState,
) -> ProvisioningAdmissionRecord:
    """Convert an internal admission value to a durable record.

    Args:
        state: Immutable global admission state.

    Returns:
        Validated durable admission record.
    """
    return ProvisioningAdmissionRecord(
        schema_version=1,
        in_flight_request_id=state.in_flight_request_id,
        last_admitted_at=state.last_admitted_at,
        next_admission_at=state.next_admission_at,
        backoff_attempt=state.backoff_attempt,
        backoff_until=state.backoff_until,
        revision=state.revision,
    )


def store_from_record(record: ProvisioningStoreRecord) -> ProvisioningStoreState:
    """Convert the atomic store record to an internal snapshot.

    Args:
        record: Validated atomic provisioning store record.

    Returns:
        Immutable store snapshot.
    """
    return ProvisioningStoreState(
        schema_version=record.schema_version,
        revision=record.revision,
        demands=tuple(demand_from_record(item) for item in record.demands),
        requests=tuple(request_from_record(item) for item in record.requests),
        admission=admission_from_record(record.admission),
        updated_at=record.updated_at,
    )


def store_to_record(state: ProvisioningStoreState) -> ProvisioningStoreRecord:
    """Convert an atomic internal snapshot to a durable record.

    Args:
        state: Immutable store snapshot.

    Returns:
        Validated atomic provisioning store record.
    """
    return ProvisioningStoreRecord(
        schema_version=1,
        revision=state.revision,
        demands=tuple(demand_to_record(item) for item in state.demands),
        requests=tuple(request_to_record(item) for item in state.requests),
        admission=admission_to_record(state.admission),
        updated_at=state.updated_at,
    )


def demand_view_from_state(
    state: ProvisioningDemandState,
) -> WorkerProvisioningDemandView:
    """Build an opaque demand view for the local Control Panel.

    Args:
        state: Internal demand state.

    Returns:
        Validated demand response view.
    """
    return WorkerProvisioningDemandView(
        project_id=state.project_id,
        desired_capacity=state.desired_capacity,
        spawn_needed=state.spawn_needed,
        binding_revision=state.binding_revision,
        revision=state.revision,
        updated_at=state.updated_at,
    )


def request_view_from_state(
    state: ProvisioningRequestState,
) -> WorkerProvisioningRequestView:
    """Build an opaque browser-safe request view.

    Args:
        state: Internal request state.

    Returns:
        Validated request response view with only provisioning identities and revisions.
    """
    return WorkerProvisioningRequestView(
        request_id=state.request_id,
        project_id=state.project_id,
        status=_REQUEST_STATUS_VALUES[state.status],
        admission_revision=state.admission_revision,
        revision=state.revision,
        adapter_metadata=metadata_schema_from_state(state.adapter_metadata),
        conversation_id=state.conversation_id,
        wake_target_id=state.wake_target_id,
        failure_reason=state.failure_reason,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )
